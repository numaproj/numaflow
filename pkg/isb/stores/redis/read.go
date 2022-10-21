package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// BufferRead is the read queue implementation powered by RedisClient.
type BufferRead struct {
	Name     string
	Stream   string
	Group    string
	Consumer string
	*BufferReadInfo
	*redisclient.RedisClient
	options
	log *zap.SugaredLogger
}

// BufferReadInfo will contain the buffer information from the reader point of view.
type BufferReadInfo struct {
	rwLock            *sync.RWMutex
	isEmpty           bool
	lag               *atomic.Duration
	refreshEmptyError *atomic.Uint32
}

var _ isb.BufferReader = (*BufferRead)(nil)

// NewBufferRead returns a new redis buffer reader.
func NewBufferRead(ctx context.Context, client *redisclient.RedisClient, name string, group string, consumer string, opts ...Option) isb.BufferReader {
	options := &options{
		infoRefreshInterval: time.Second,
		readTimeOut:         time.Second,
		checkBackLog:        true,
	}

	for _, o := range opts {
		o.apply(options)
	}

	rqr := &BufferRead{
		Name:        name,
		Stream:      redisclient.GetRedisStreamName(name),
		Group:       group,
		Consumer:    consumer,
		RedisClient: client,
		BufferReadInfo: &BufferReadInfo{
			rwLock:            new(sync.RWMutex),
			isEmpty:           true,
			lag:               atomic.NewDuration(0),
			refreshEmptyError: atomic.NewUint32(0),
		},
		options: *options,
		// checkBackLog is set to true as on start up we need to start from the beginning
	}
	rqr.log = logging.FromContext(ctx).With("BufferReader", rqr.GetName())
	// updateIsEmptyFlag is used to update isEmpty flag once
	rqr.updateIsEmptyFlag(ctx)

	// refresh IsEmpty Flag  at a periodic interval
	go rqr.refreshIsEmptyFlag(ctx)
	return rqr
}

// refreshIsEmptyFlag is used to refresh the changes for isEmpty
func (br *BufferRead) refreshIsEmptyFlag(ctx context.Context) {
	ticker := time.NewTicker(br.options.infoRefreshInterval)
	defer ticker.Stop()
	br.log.Infow("refreshIsEmptyFlag has started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			br.updateIsEmptyFlag(ctx)
		}
	}
}

func (br *BufferRead) Close() error {
	return nil
}

// IsEmpty returns whether the buffer is empty.
func (br *BufferRead) IsEmpty() bool {
	br.BufferReadInfo.rwLock.RLock()
	defer br.BufferReadInfo.rwLock.RUnlock()
	return br.BufferReadInfo.isEmpty
}

// GetRefreshEmptyError returns whether the buffer has a lag
func (br *BufferRead) GetRefreshEmptyError() uint32 {
	return br.BufferReadInfo.refreshEmptyError.Load()
}

// GetLag returns the lag of the buffer
func (br *BufferRead) GetLag() time.Duration {
	return br.BufferReadInfo.lag.Load()
}

// updateIsEmptyFlag is used to check if the buffer is empty. Lag of even one consumer is considered a lag.
// We obtain the last generated Id for a stream using the XInfo command which is the last id added to the stream,
// We also obtain the list of all the delivered ids for all the different consumer groups.
// We assume that if the difference of the lastGeneratedId and lastDelivered = 0 the buffer is considered empty
// It is tough to get the exact numbers https://github.com/redis/redis/issues/8392
func (br *BufferRead) updateIsEmptyFlag(_ context.Context) {
	ctx := redisclient.RedisContext
	infoStream := br.Client.XInfoStream(ctx, br.GetStreamName())

	labels := map[string]string{"buffer": br.GetName()}

	// Explicitly set error to nil if there
	if infoStream.Err() != nil && infoStream.Err().Error() == "ERR no such key" {
		br.setError("Stream not found", infoStream.Err())
		isbIsEmptyFlagErrors.With(labels).Inc()
		return
	}

	if infoStream.Err() != nil {
		br.setError("XInfoStream error in updateIsEmptyFlag", infoStream.Err())
		isbIsEmptyFlagErrors.With(labels).Inc()
		return
	}

	lastGenerated := infoStream.Val().LastGeneratedID

	lastGeneratedId, err := splitId(lastGenerated)

	if err != nil {
		br.setError("Error in updateIsEmptyFlag", err)
		isbIsEmptyFlagErrors.With(labels).Inc()
		return
	}

	xInfoGroups := br.Client.XInfoGroups(ctx, br.GetStreamName())

	results := xInfoGroups.Val()

	if len(results) == 0 {
		br.setError("No consumers groups found", err)
		isbIsEmptyFlagErrors.With(labels).Inc()
		return
	}

	var lastDeliveredId int64

	var lastDelivered string

	for _, result := range results {
		if result.Name == br.GetGroupName() {
			lastDelivered = result.LastDeliveredID
			lastDeliveredId, err = splitId(lastDelivered)
			if err != nil {
				br.setError("Error in updateIsEmptyFlag", err)
				return
			}
		}
	}

	// Set the refresh empty error to 0
	br.BufferReadInfo.refreshEmptyError.Store(0)
	// obtain current lag
	currentLag := time.UnixMilli(lastGeneratedId).Sub(time.UnixMilli(lastDeliveredId))
	// Get Previous lag and set the current lag
	previousLag := br.GetLag()
	// Set the difference between current and previous lag (+ve increasing lag, -ve decreasing lag)
	br.BufferReadInfo.lag.Store(currentLag - previousLag)

	// string comparison of lastGenerated and lastDelivered
	if lastGenerated == lastDelivered {
		br.log.Debugw("Is Empty")
		br.setIsEmptyFlag(true)
		isbIsEmpty.With(labels).Inc()
		return
	}

	br.setIsEmptyFlag(false)
}

// setIsEmptyFlag is used to set isEmpty flag to true
func (br *BufferRead) setIsEmptyFlag(flag bool) {
	br.BufferReadInfo.rwLock.Lock()
	defer br.BufferReadInfo.rwLock.Unlock()
	br.BufferReadInfo.isEmpty = flag
}

// setError is used to set error cases
func (br *BufferRead) setError(errMsg string, err error) {
	br.log.Errorw(errMsg, zap.Error(err))
	br.BufferReadInfo.refreshEmptyError.Inc()
	br.setIsEmptyFlag(false)
}

// Read reads the messages from the stream.
// During a restart, we need to make sure all the un-acknowledged messages are reprocessed.
// we need to replace `>` with `0-0` during restarts. We might run into data loss otherwise.
func (br *BufferRead) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var messages = make([]*isb.ReadMessage, 0, count)
	var xstreams []redis.XStream
	var err error
	// start with 0-0 if checkBackLog is true
	labels := map[string]string{"buffer": br.GetName()}
	if br.options.checkBackLog {
		xstreams, err = br.processXReadResult("0-0", count)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
				br.log.Debugw("checkBacklog true, redis.Nil", zap.Error(err))
				return messages, nil
			}
			isbReadErrors.With(labels).Inc()
			// we should try to do our best effort to convert our data here, if there is data available in xstream from the previous loop
			messages, errMsg := br.convertXStreamToMessages(xstreams, messages, labels)
			br.log.Errorw("checkBacklog true, convertXStreamToMessages failed", zap.Error(errMsg))
			return messages, fmt.Errorf("XReadGroup failed, %w", err)
		}

		// NOTE: If all messages have been delivered and acknowledged, the XREADGROUP 0-0 call returns an empty
		// list of messages in the stream. At this point we want to read everything from last delivered which would be >
		if len(xstreams) == 1 && len(xstreams[0].Messages) == 0 {
			br.log.Infow("We have delivered and acknowledged all PENDING msgs, setting checkBacklog to false")
			br.checkBackLog = false
		}
	}
	if !br.options.checkBackLog {
		xstreams, err = br.processXReadResult(">", count)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
				br.log.Debugw("checkBacklog false, redis.Nil", zap.Error(err))
				return messages, nil
			}
			isbReadErrors.With(labels).Inc()
			// we should try to do our best effort to convert our data here, if there is data available in xstream from the previous loop
			messages, errMsg := br.convertXStreamToMessages(xstreams, messages, labels)
			br.log.Errorw("checkBacklog false, convertXStreamToMessages failed", zap.Error(errMsg))
			return messages, fmt.Errorf("XReadGroup failed, %w", err)
		}
	}

	// for each XMessage in []XStream
	return br.convertXStreamToMessages(xstreams, messages, labels)
}

// Ack acknowledges the offset to the read queue. Ack is always pipelined, if you want to avoid it then
// send array of 1 element.
func (br *BufferRead) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	strOffsets := []string{}
	for _, o := range offsets {
		strOffsets = append(strOffsets, o.String())
	}
	if err := br.Client.XAck(redisclient.RedisContext, br.Stream, br.Group, strOffsets...).Err(); err != nil {
		for i := 0; i < len(offsets); i++ {
			errs[i] = err
		}
	}
	return errs
}

// processXReadResult is used to process the results of XREADGROUP
func (br *BufferRead) processXReadResult(startIndex string, count int64) ([]redis.XStream, error) {
	result := br.Client.XReadGroup(redisclient.RedisContext, &redis.XReadGroupArgs{
		Group:    br.Group,
		Consumer: br.Consumer,
		Streams:  []string{br.Stream, startIndex},
		Count:    count,
		Block:    br.options.readTimeOut,
	})
	return result.Result()
}

// convertXStreamToMessages is used to convert xstreams to messages
func (br *BufferRead) convertXStreamToMessages(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
	// for each XMessage in []XStream
	for _, xstream := range xstreams {
		for _, message := range xstream.Messages {
			var readOffset = message.ID

			// our messages have only one field/value pair (i.e., header/payload)
			if len(message.Values) != 1 {
				isbReadErrors.With(labels).Inc()
				return messages, fmt.Errorf("expected only 1 pair of field/value in stream %+v", message.Values)
			}
			for f, v := range message.Values {
				msg, err := getHeaderAndBody(f, v)
				if err != nil {
					return messages, fmt.Errorf("%w", err)
				}
				readMessage := isb.ReadMessage{
					Message:    msg,
					ReadOffset: isb.SimpleStringOffset(func() string { return readOffset }),
				}
				messages = append(messages, &readMessage)
			}
		}
	}

	return messages, nil
}

func (br *BufferRead) Pending(_ context.Context) (int64, error) {
	// TODO: not implemented
	return isb.PendingNotAvailable, nil
}

func (br *BufferRead) Rate(_ context.Context) (float64, error) {
	// TODO: not implemented
	return isb.RateNotAvailable, nil
}

func getHeaderAndBody(field string, value interface{}) (msg isb.Message, err error) {
	err = msg.Header.UnmarshalBinary([]byte(field))
	if err != nil {
		return msg, fmt.Errorf("header unmarshal error %w", err)
	}

	msg.Body.Payload = []byte(value.(string))
	return msg, nil
}

// GetName returns name for the buffer.
func (br *BufferRead) GetName() string {
	return br.Name
}

// GetStreamName returns the stream name.
func (br *BufferRead) GetStreamName() string {
	return br.Stream
}

// GetGroupName gets the name of the consumer group.
func (br *BufferRead) GetGroupName() string {
	return br.Group
}

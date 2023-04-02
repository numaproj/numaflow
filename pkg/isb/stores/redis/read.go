/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"context"
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
	*redisclient.RedisStreamsRead
	*BufferReadInfo
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
func NewBufferRead(ctx context.Context, client *redisclient.RedisClient, name string, group string, consumer string, opts ...redisclient.Option) isb.BufferReader {
	options := &redisclient.Options{
		InfoRefreshInterval: time.Second,
		ReadTimeOut:         time.Second,
		CheckBackLog:        true,
	}

	for _, o := range opts {
		o.Apply(options)
	}

	rqr := &BufferRead{
		RedisStreamsRead: &redisclient.RedisStreamsRead{
			Name:        name,
			Stream:      redisclient.GetRedisStreamName(name),
			Group:       group,
			Consumer:    consumer,
			RedisClient: client,
			Options:     *options,
			Metrics: redisclient.Metrics{
				ReadErrorsInc: func() {
					labels := map[string]string{"buffer": name}
					isbReadErrors.With(labels).Inc()
				},
			},
		},
		BufferReadInfo: &BufferReadInfo{
			rwLock:            new(sync.RWMutex),
			isEmpty:           true,
			lag:               atomic.NewDuration(0),
			refreshEmptyError: atomic.NewUint32(0),
		},
		// checkBackLog is set to true as on start up we need to start from the beginning
	}

	// this function describes how to derive messages from the XSstream
	rqr.XStreamToMessages = func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
		// for each XMessage in []XStream
		for _, xstream := range xstreams {
			for _, message := range xstream.Messages {
				var readOffset = message.ID

				// our messages have only one field/value pair (i.e., header/payload)
				if len(message.Values) != 1 {
					if rqr.Metrics.ReadErrorsInc != nil {
						rqr.Metrics.ReadErrorsInc()
					}
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

	rqr.Log = logging.FromContext(ctx).With("BufferReader", rqr.GetName())
	// updateIsEmptyFlag is used to update isEmpty flag once
	rqr.updateIsEmptyFlag(ctx)

	// refresh IsEmpty Flag  at a periodic interval
	go rqr.refreshIsEmptyFlag(ctx)
	return rqr
}

func getHeaderAndBody(field string, value interface{}) (msg isb.Message, err error) {
	err = msg.Header.UnmarshalBinary([]byte(field))
	if err != nil {
		return msg, fmt.Errorf("header unmarshal error %w", err)
	}

	msg.Body.Payload = []byte(value.(string))
	return msg, nil
}

// refreshIsEmptyFlag is used to refresh the changes for isEmpty
func (br *BufferRead) refreshIsEmptyFlag(ctx context.Context) {
	ticker := time.NewTicker(br.Options.InfoRefreshInterval)
	defer ticker.Stop()
	br.Log.Infow("refreshIsEmptyFlag has started")
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
		br.Log.Debugw("Is Empty")
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
	br.Log.Errorw(errMsg, zap.Error(err))
	br.BufferReadInfo.refreshEmptyError.Inc()
	br.setIsEmptyFlag(false)
}

func (br *BufferRead) Pending(_ context.Context) (int64, error) {
	// TODO: not implemented
	return isb.PendingNotAvailable, nil
}

func (br *BufferRead) Rate(_ context.Context) (float64, error) {
	// TODO: not implemented
	return isb.RateNotAvailable, nil
}

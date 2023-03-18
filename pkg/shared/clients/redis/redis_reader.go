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
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	//redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

// RedisStreamsReader is the read queue implementation powered by RedisClient.
type RedisStreamsReader struct {
	Name     string
	Stream   string
	Group    string
	Consumer string

	*RedisClient
	Options
	Log     *zap.SugaredLogger
	Metrics Metrics
}

type Metrics struct {
	ReadErrors *prometheus.CounterVec
	Reads      *prometheus.CounterVec //todo: use this
	Acks       *prometheus.CounterVec //todo: use this
}

func (br *RedisStreamsReader) GetName() string {
	return br.Name
}

// GetStreamName returns the stream name.
func (br *RedisStreamsReader) GetStreamName() string {
	return br.Stream
}

// GetGroupName gets the name of the consumer group.
func (br *RedisStreamsReader) GetGroupName() string {
	return br.Group
}

/*
func NewRedisStreamsReader(name string, group string, consumer string, client *RedisClient, metrics Metrics) *RedisStreamsReader {
	Options := &Options{ // TODO: if we don't end up needing this outside of this package we can turn it back to "options"
		InfoRefreshInterval: time.Second,
		ReadTimeOut:         time.Second,
		CheckBackLog:        true,
	}

	for _, o := range opts {
		o.Apply(Options)
	}

	return &RedisStreamsReader{
		Name:        name,
		Stream:      GetRedisStreamName(name),
		Group:       group,
		Consumer:    consumer,
		RedisClient: client,
		Metrics:     metrics}
}*/

// Read reads the messages from the stream.
// During a restart, we need to make sure all the un-acknowledged messages are reprocessed.
// we need to replace `>` with `0-0` during restarts. We might run into data loss otherwise.
func (br *RedisStreamsReader) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var messages = make([]*isb.ReadMessage, 0, count)
	var xstreams []redis.XStream
	var err error
	// start with 0-0 if CheckBackLog is true
	labels := map[string]string{"buffer": br.GetName()}
	if br.Options.CheckBackLog {
		xstreams, err = br.processXReadResult("0-0", count)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
				br.Log.Debugw("checkBacklog true, redis.Nil", zap.Error(err))
				return messages, nil
			}
			if br.Metrics.ReadErrors != nil {
				br.Metrics.ReadErrors.With(labels).Inc()
			}
			// we should try to do our best effort to convert our data here, if there is data available in xstream from the previous loop
			messages, errMsg := br.convertXStreamToMessages(xstreams, messages, labels)
			br.Log.Errorw("checkBacklog true, convertXStreamToMessages failed", zap.Error(errMsg))
			return messages, fmt.Errorf("XReadGroup failed, %w", err)
		}

		// NOTE: If all messages have been delivered and acknowledged, the XREADGROUP 0-0 call returns an empty
		// list of messages in the stream. At this point we want to read everything from last delivered which would be >
		if len(xstreams) == 1 && len(xstreams[0].Messages) == 0 {
			br.Log.Infow("We have delivered and acknowledged all PENDING msgs, setting checkBacklog to false")
			br.CheckBackLog = false
		}
	}
	if !br.Options.CheckBackLog {
		xstreams, err = br.processXReadResult(">", count)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
				br.Log.Debugw("checkBacklog false, redis.Nil", zap.Error(err))
				return messages, nil
			}
			if br.Metrics.ReadErrors != nil {
				br.Metrics.ReadErrors.With(labels).Inc()
			}
			// we should try to do our best effort to convert our data here, if there is data available in xstream from the previous loop
			messages, errMsg := br.convertXStreamToMessages(xstreams, messages, labels)
			br.Log.Errorw("checkBacklog false, convertXStreamToMessages failed", zap.Error(errMsg))
			return messages, fmt.Errorf("XReadGroup failed, %w", err)
		}
	}

	// for each XMessage in []XStream
	return br.convertXStreamToMessages(xstreams, messages, labels)
}

// Ack acknowledges the offset to the read queue. Ack is always pipelined, if you want to avoid it then
// send array of 1 element.
func (br *RedisStreamsReader) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	strOffsets := []string{}
	for _, o := range offsets {
		strOffsets = append(strOffsets, o.String())
	}
	if err := br.Client.XAck(RedisContext, br.Stream, br.Group, strOffsets...).Err(); err != nil {
		for i := 0; i < len(offsets); i++ {
			errs[i] = err
		}
	}
	return errs
}

// processXReadResult is used to process the results of XREADGROUP
func (br *RedisStreamsReader) processXReadResult(startIndex string, count int64) ([]redis.XStream, error) {
	result := br.Client.XReadGroup(RedisContext, &redis.XReadGroupArgs{
		Group:    br.Group,
		Consumer: br.Consumer,
		Streams:  []string{br.Stream, startIndex},
		Count:    count,
		Block:    br.Options.ReadTimeOut,
	})
	return result.Result()
}

// convertXStreamToMessages is used to convert xstreams to messages
func (br *RedisStreamsReader) convertXStreamToMessages(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
	// for each XMessage in []XStream
	for _, xstream := range xstreams {
		for _, message := range xstream.Messages {
			var readOffset = message.ID

			// our messages have only one field/value pair (i.e., header/payload)
			if len(message.Values) != 1 {
				if br.Metrics.ReadErrors != nil {
					br.Metrics.ReadErrors.With(labels).Inc()
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

func getHeaderAndBody(field string, value interface{}) (msg isb.Message, err error) {
	err = msg.Header.UnmarshalBinary([]byte(field))
	if err != nil {
		return msg, fmt.Errorf("header unmarshal error %w", err)
	}

	msg.Body.Payload = []byte(value.(string))
	return msg, nil
}

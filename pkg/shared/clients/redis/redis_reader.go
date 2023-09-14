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

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
)

// RedisStreamsRead is the read queue implementation powered by RedisClient.
type RedisStreamsRead struct {
	Name         string
	Stream       string
	Group        string
	Consumer     string
	PartitionIdx int32

	*RedisClient
	Options
	Log     *zap.SugaredLogger
	Metrics Metrics

	XStreamToMessages func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error)
}

type Metrics struct {
	ReadErrorsInc metricsIncrementFunc
	ReadsAdd      metricsAddFunc // number of actual messages read in
	AcksAdd       metricsAddFunc
	AckErrorsAdd  metricsAddFunc
}

// need a function type which increments a particular counter
type metricsIncrementFunc func()
type metricsAddFunc func(int)

// GetName returns the name of the partitioned buffer.
func (br *RedisStreamsRead) GetName() string {
	return br.Name
}

// GetPartitionIdx returns the partition number.
func (br *RedisStreamsRead) GetPartitionIdx() int32 {
	return br.PartitionIdx
}

// GetStreamName returns the stream name.
func (br *RedisStreamsRead) GetStreamName() string {
	return br.Stream
}

// GetGroupName gets the name of the consumer group.
func (br *RedisStreamsRead) GetGroupName() string {
	return br.Group
}

// Read reads the messages from the stream.
// During a restart, we need to make sure all the un-acknowledged messages are reprocessed.
// we need to replace `>` with `0-0` during restarts. We might run into data loss otherwise.
func (br *RedisStreamsRead) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var messages = make([]*isb.ReadMessage, 0, count)
	xstreams := []redis.XStream{}
	var err error

	// start with 0-0 if CheckBackLog is true
	labels := map[string]string{"buffer": br.GetName()}
	if br.Options.CheckBackLog {
		xstreams, err = br.processXReadResult("0-0", count)
		if err != nil {
			return br.processReadError(xstreams, messages, err)
		}

		// NOTE: If all messages have been delivered and acknowledged, the XREADGROUP 0-0 call returns an empty
		// list of messages in the stream. At this point we want to read everything from last delivered which would be >
		if len(xstreams) == 1 && len(xstreams[0].Messages) == 0 {
			br.Log.Infow("We have delivered and acknowledged all PENDING msgs, setting checkBacklog to false")
			br.CheckBackLog = false
		}
	}
	if !br.Options.CheckBackLog {
		streams, err := br.processXReadResult(">", count)
		xstreams = append(xstreams, streams...)
		if err != nil {
			return br.processReadError(xstreams, messages, err)
		}
	}

	// Update metric for number of messages read in
	for _, xstream := range xstreams {
		if br.Metrics.ReadsAdd != nil {
			br.Metrics.ReadsAdd(len(xstream.Messages))
		}
	}
	// Generate messages from the XStream
	msgs, err := br.XStreamToMessages(xstreams, messages, labels)
	br.Log.Debugf("Received %d messages over Redis Streams Source, err=%v", len(msgs), err)
	return msgs, err
}

func (br *RedisStreamsRead) processReadError(xstreams []redis.XStream, messages []*isb.ReadMessage, err error) ([]*isb.ReadMessage, error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
		br.Log.Debugf("redis.Nil/context cancelled, checkBackLog=%v, err=%v", br.Options.CheckBackLog, err)
		return messages, nil
	}

	if br.Metrics.ReadErrorsInc != nil {
		br.Metrics.ReadErrorsInc()
	}
	// we should try to do our best effort to convert our data here, if there is data available in xstream from the previous loop
	messages, errMsg := br.XStreamToMessages(xstreams, messages, map[string]string{"buffer": br.GetName()})
	br.Log.Errorf("convertXStreamToMessages failed, checkBackLog=%v, err=%s", br.Options.CheckBackLog, errMsg)
	return messages, fmt.Errorf("XReadGroup failed, %w", err)
}

// Ack acknowledges the offset to the read queue. Ack is always pipelined, if you want to avoid it then
// send array of 1 element.
func (br *RedisStreamsRead) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	// if we were to have n messages produced from 1 incoming message, we could have
	// the same offset more than once: just in case, we can deduplicate
	dedupOffsets := make(map[string]struct{}) // essentially a Set
	strOffsets := []string{}
	for _, o := range offsets {
		// for redis, we don't consider partition id to ack
		ofs := o.String()
		_, found := dedupOffsets[ofs]
		if !found {
			dedupOffsets[ofs] = struct{}{}
			strOffsets = append(strOffsets, ofs)
		}
	}
	if err := br.Client.XAck(RedisContext, br.Stream, br.Group, strOffsets...).Err(); err != nil {
		for i := 0; i < len(offsets); i++ {
			errs[i] = err // 'errs' is indexed the same as 'offsets'
		}
		if br.Metrics.AckErrorsAdd != nil {
			br.Metrics.AckErrorsAdd(len(strOffsets))
		}
	} else {
		if br.Metrics.AcksAdd != nil {
			br.Metrics.AcksAdd(len(strOffsets))
		}
	}
	return errs
}

func (br *RedisStreamsRead) NoAck(_ context.Context, _ []isb.Offset) {}

func (br *RedisStreamsRead) Pending(_ context.Context) (int64, error) {
	// try calling XINFO GROUPS <stream> and look for 'Lag' key.
	// For Redis Server < v7.0, this always returns 0; therefore it's recommended to use >= v7.0

	result := br.Client.XInfoGroups(RedisContext, br.Stream)
	groups, err := result.Result()
	if err != nil {
		return isb.PendingNotAvailable, fmt.Errorf("error calling XInfoGroups: %v", err)
	}
	// find our ConsumerGroup
	for _, group := range groups {
		if group.Name == br.Group {
			return group.Lag, nil
		}
	}
	return isb.PendingNotAvailable, fmt.Errorf("ConsumerGroup %q not found in XInfoGroups result %+v", br.Group, groups)
}

// processXReadResult is used to process the results of XREADGROUP
func (br *RedisStreamsRead) processXReadResult(startIndex string, count int64) ([]redis.XStream, error) {
	result := br.Client.XReadGroup(RedisContext, &redis.XReadGroupArgs{
		Group:    br.Group,
		Consumer: br.Consumer,
		Streams:  []string{br.Stream, startIndex},
		Count:    count,
		Block:    br.Options.ReadTimeOut,
	})
	return result.Result()
}

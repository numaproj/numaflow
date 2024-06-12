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
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// exactlyOnceHashWindow groups a set of time range to a single bucket
const exactlyOnceHashWindow = time.Minute * 5

//go:embed exactlyOnceInsert.lua
var exactlyOnceInsertLuaScript string

// BufferWrite is the write queue implementation powered by RedisClient.
type BufferWrite struct {
	Name         string
	PartitionIdx int32
	Stream       string
	Group        string
	*BufferWriteInfo
	*redisclient.RedisClient
	redisclient.Options
	log *zap.SugaredLogger
}

func (bw *BufferWrite) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	//TODO implement me
	panic("implement me")
}

// BufferWriteInfo will contain the buffer infoRefreshInterval from the writer point of view.
type BufferWriteInfo struct {
	isFull           *atomic.Bool
	consumerLag      *atomic.Duration
	refreshFullError *atomic.Uint32
	minId            *atomic.String
	bufferLength     *atomic.Int64
	pendingCount     *atomic.Int64
	// hasUnprocessedData indicates if there is any unprocessed data left in the buffer
	hasUnprocessedData *atomic.Bool
}

var _ isb.BufferWriter = (*BufferWrite)(nil)

// NewBufferWrite returns a new redis queue writer.
func NewBufferWrite(ctx context.Context, client *redisclient.RedisClient, name string, group string, partitionIdx int32, opts ...redisclient.Option) isb.BufferWriter {
	options := &redisclient.Options{
		Pipelining:                true,
		InfoRefreshInterval:       time.Second,
		LagDuration:               time.Duration(0),
		MaxLength:                 dfv1.DefaultBufferLength,
		BufferUsageLimit:          dfv1.DefaultBufferUsageLimit,
		RefreshBufferWriteInfo:    true,
		BufferFullWritingStrategy: dfv1.RetryUntilSuccess,
	}

	for _, o := range opts {
		o.Apply(options)
	}

	// check whether the script exists, if not then load
	rqw := &BufferWrite{
		Name:         name,
		PartitionIdx: partitionIdx,
		Stream:       redisclient.GetRedisStreamName(name),
		Group:        group,
		BufferWriteInfo: &BufferWriteInfo{
			isFull:           atomic.NewBool(true),
			refreshFullError: atomic.NewUint32(0),
			consumerLag:      atomic.NewDuration(0),
			minId:            atomic.NewString("0-0"),
			// During start up if we set pending count to 0 we are saying nothing is pending.
			pendingCount:       atomic.NewInt64(options.MaxLength),
			bufferLength:       atomic.NewInt64(options.MaxLength),
			hasUnprocessedData: atomic.NewBool(true),
		},
		RedisClient: client,
	}
	rqw.Options = *options

	rqw.log = logging.FromContext(ctx).With("bufferWriter", rqw.GetName())

	// setWriteInfo is used to update isFull flag and minId once
	rqw.setWriteInfo(ctx)

	if rqw.Options.RefreshBufferWriteInfo {
		// refresh isFull flag at a periodic interval
		go rqw.refreshWriteInfo(ctx)
	}

	return rqw
}

// refreshWriteInfo is used to refresh the changes
func (bw *BufferWrite) refreshWriteInfo(ctx context.Context) {
	ticker := time.NewTicker(bw.Options.InfoRefreshInterval)
	defer ticker.Stop()
	bw.log.Infow("refreshWriteInfo has started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bw.setWriteInfo(ctx)

			if bw.isFull.Load() {
				// execute XTRIM with MINID to avoid the deadlock since default queue purging happens on XADD and
				// whenever a buffer is full XADD is never invoked
				bw.trim(ctx)
			}
		}
	}
}

// trim is used to explicitly call TRIM on buffer being full
func (bw *BufferWrite) trim(_ context.Context) {
	ctx := redisclient.RedisContext
	bw.log.Infow("Explicit trim on MINID", zap.String("MINID", bw.minId.Load()))
	result := bw.Client.XTrimMinID(ctx, bw.GetStreamName(), bw.minId.Load())

	if result.Err() != nil {
		bw.log.Errorw("XTRIM failed", zap.Error(result.Err()))
	}
}

func (br *BufferWrite) Close() error {
	return nil
}

// Write is used to write data to the redis interstep buffer
func (bw *BufferWrite) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	ctx := redisclient.RedisContext
	var errs = make([]error, len(messages))

	script := redis.NewScript(exactlyOnceInsertLuaScript)
	labels := map[string]string{"buffer": bw.GetName()}

	if bw.IsFull() {
		bw.log.Debugw("Is full")
		isbIsFull.With(labels).Inc()

		// when buffer is full, we need to decide whether to discard the message or not.
		switch bw.BufferFullWritingStrategy {
		case dfv1.DiscardLatest:
			// user explicitly wants to discard the message when buffer if full.
			// return no retryable error as a callback to let caller know that the message is discarded.
			initializeErrorArray(errs, isb.NonRetryableBufferWriteErr{Name: bw.Name, Message: isb.BufferFullMessage})
		default:
			// Default behavior is to return a BufferWriteErr.
			initializeErrorArray(errs, isb.BufferWriteErr{Name: bw.Name, Full: true, Message: isb.BufferFullMessage})
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}
	// Maybe just do pipelined write, always?
	if !bw.Pipelining {
		for idx, message := range messages {
			// Reference the Payload in Body directly when writing to Redis ISB to avoid extra marshaling.
			// TODO: revisit directly Payload reference when Body structure changes
			errs[idx] = script.Run(ctx, bw.Client, []string{bw.GetHashKeyName(message.EventTime), bw.Stream}, message.Header.ID, message.Header, message.Body.Payload, bw.BufferWriteInfo.minId.String()).Err()
		}
	} else {
		var scriptMissing bool
		// use pipelining
		errs, scriptMissing = bw.pipelinedWrite(ctx, script, messages)
		// if scriptMissing, then load and retry
		if scriptMissing {
			if err := bw.Client.ScriptLoad(ctx, exactlyOnceInsertLuaScript).Err(); err != nil {
				initializeErrorArray(errs, err)
				isbWriteErrors.With(labels).Inc()
				return nil, errs
			}
			// now that we have loaded, we do not care about whether the script exists or not.
			errs, _ = bw.pipelinedWrite(ctx, script, messages)
		}
	}

	return nil, errs
}

// initializeErrorArray is used to initialize an empty array for
func initializeErrorArray(errs []error, err error) {
	for i := range errs {
		errs[i] = err
	}
}

// pipelinedWrite is used to perform pipelined write messages
func (bw *BufferWrite) pipelinedWrite(ctx context.Context, script *redis.Script, messages []isb.Message) ([]error, bool) {
	var errs = make([]error, len(messages))
	var cmds = make([]*redis.Cmd, len(messages))
	pipe := bw.Client.Pipeline()

	for idx, message := range messages {
		// Reference the Payload in Body directly when writing to Redis ISB to avoid extra marshaling.
		// TODO: revisit directly Payload reference when Body structure changes
		cmds[idx] = script.Run(ctx, pipe, []string{bw.GetHashKeyName(message.EventTime), bw.Stream}, message.Header.ID, message.Header, message.Body.Payload, bw.BufferWriteInfo.minId.String())
	}

	scriptMissing := false
	_, err := pipe.Exec(ctx)
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			scriptMissing = true
		}
		initializeErrorArray(errs, err)
		return errs, scriptMissing
	}

	for idx, cmd := range cmds {
		err := cmd.Err()
		if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			scriptMissing = true
		}
		errs[idx] = err
	}

	return errs, scriptMissing
}

// GetHashKeyName gets the hash key name.
func (bw *BufferWrite) GetHashKeyName(startTime time.Time) string {
	return fmt.Sprintf("%s-h-%d", bw.Stream, startTime.Truncate(exactlyOnceHashWindow).Unix())
}

// GetStreamName gets the stream name. Stream name is derived from the name.
func (bw *BufferWrite) GetStreamName() string {
	return bw.Stream
}

// GetName gets the name of the buffer.
func (bw *BufferWrite) GetName() string {
	return bw.Name
}

func (bw *BufferWrite) GetPartitionIdx() int32 {
	return bw.PartitionIdx
}

// GetGroupName gets the name of the consumer group.
func (bw *BufferWrite) GetGroupName() string {
	return bw.Group
}

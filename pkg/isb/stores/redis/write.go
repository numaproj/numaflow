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

// RedisWriter is the write queue implementation powered by RedisClient.
type RedisWriter struct {
	Name         string
	PartitionIdx int32
	Stream       string
	Group        string
	*PartitionWriteInfo
	*redisclient.RedisClient
	redisclient.Options
	log *zap.SugaredLogger
}

// PartitionWriteInfo will contain the partitoin infoRefreshInterval from the writer point of view.
type PartitionWriteInfo struct {
	isFull           *atomic.Bool
	consumerLag      *atomic.Duration
	refreshFullError *atomic.Uint32
	minId            *atomic.String
	partitionLength  *atomic.Int64
	pendingCount     *atomic.Int64
	// hasUnprocessedData indicates if there is any unprocessed data left in the partition
	hasUnprocessedData *atomic.Bool
}

var _ isb.PartitionWriter = (*RedisWriter)(nil)

// NewPartitionWrite returns a new redis queue writer.
func NewPartitionWrite(ctx context.Context, client *redisclient.RedisClient, name string, group string, partitionIdx int32, opts ...redisclient.Option) isb.PartitionWriter {
	options := &redisclient.Options{
		Pipelining:                   true,
		InfoRefreshInterval:          time.Second,
		LagDuration:                  time.Duration(0),
		MaxLength:                    dfv1.DefaultPartitionLength,
		PartitionUsageLimit:          dfv1.DefaultPartitionUsageLimit,
		RefreshPartitionWriteInfo:    true,
		PartitionFullWritingStrategy: dfv1.RetryUntilSuccess,
	}

	for _, o := range opts {
		o.Apply(options)
	}

	// check whether the script exists, if not then load
	rqw := &RedisWriter{
		Name:         name,
		PartitionIdx: partitionIdx,
		Stream:       redisclient.GetRedisStreamName(name),
		Group:        group,
		PartitionWriteInfo: &PartitionWriteInfo{
			isFull:           atomic.NewBool(true),
			refreshFullError: atomic.NewUint32(0),
			consumerLag:      atomic.NewDuration(0),
			minId:            atomic.NewString("0-0"),
			// During start up if we set pending count to 0 we are saying nothing is pending.
			pendingCount:       atomic.NewInt64(options.MaxLength),
			partitionLength:    atomic.NewInt64(options.MaxLength),
			hasUnprocessedData: atomic.NewBool(true),
		},
		RedisClient: client,
	}
	rqw.Options = *options

	rqw.log = logging.FromContext(ctx).With("partitionWriter", rqw.GetName())

	// setWriteInfo is used to update isFull flag and minId once
	rqw.setWriteInfo(ctx)

	if rqw.Options.RefreshPartitionWriteInfo {
		// refresh isFull flag at a periodic interval
		go rqw.refreshWriteInfo(ctx)
	}

	return rqw
}

// refreshWriteInfo is used to refresh the changes
func (pw *RedisWriter) refreshWriteInfo(ctx context.Context) {
	ticker := time.NewTicker(pw.Options.InfoRefreshInterval)
	defer ticker.Stop()
	pw.log.Infow("refreshWriteInfo has started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pw.setWriteInfo(ctx)

			if pw.isFull.Load() {
				// execute XTRIM with MINID to avoid the deadlock since default queue purging happens on XADD and
				// whenever a partition is full XADD is never invoked
				pw.trim(ctx)
			}
		}
	}
}

// trim is used to explicitly call TRIM on partition being full
func (pw *RedisWriter) trim(_ context.Context) {
	ctx := redisclient.RedisContext
	pw.log.Infow("Explicit trim on MINID", zap.String("MINID", pw.minId.Load()))
	result := pw.Client.XTrimMinID(ctx, pw.GetStreamName(), pw.minId.Load())

	if result.Err() != nil {
		pw.log.Errorw("XTRIM failed", zap.Error(result.Err()))
	}
}

func (pw *RedisWriter) Close() error {
	return nil
}

// Write is used to write data to the redis interstep partition
func (pw *RedisWriter) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	ctx := redisclient.RedisContext
	var errs = make([]error, len(messages))

	script := redis.NewScript(exactlyOnceInsertLuaScript)
	labels := map[string]string{"buffer": pw.GetName()}

	if pw.IsFull() {
		pw.log.Debugw("Is full")
		isbIsFull.With(labels).Inc()

		// when partition is full, we need to decide whether to discard the message or not.
		switch pw.PartitionFullWritingStrategy {
		case dfv1.DiscardLatest:
			// user explicitly wants to discard the message when buffer if full.
			// return no retryable error as a callback to let caller know that the message is discarded.
			initializeErrorArray(errs, isb.NonRetryablePartitionWriteErr{Name: pw.Name, Message: "Partition full!"})
		default:
			// Default behavior is to return a PartitionWriteErr.
			initializeErrorArray(errs, isb.PartitionWriteErr{Name: pw.Name, Full: true, Message: "Partition full!"})
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}
	// Maybe just do pipelined write, always?
	if !pw.Pipelining {
		for idx, message := range messages {
			// Reference the Payload in Body directly when writing to Redis ISB to avoid extra marshaling.
			// TODO: revisit directly Payload reference when Body structure changes
			errs[idx] = script.Run(ctx, pw.Client, []string{pw.GetHashKeyName(message.EventTime), pw.Stream}, message.Header.ID, message.Header, message.Body.Payload, pw.PartitionWriteInfo.minId.String()).Err()
		}
	} else {
		var scriptMissing bool
		// use pipelining
		errs, scriptMissing = pw.pipelinedWrite(ctx, script, messages)
		// if scriptMissing, then load and retry
		if scriptMissing {
			if err := pw.Client.ScriptLoad(ctx, exactlyOnceInsertLuaScript).Err(); err != nil {
				initializeErrorArray(errs, err)
				isbWriteErrors.With(labels).Inc()
				return nil, errs
			}
			// now that we have loaded, we do not care about whether the script exists or not.
			errs, _ = pw.pipelinedWrite(ctx, script, messages)
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
func (pw *RedisWriter) pipelinedWrite(ctx context.Context, script *redis.Script, messages []isb.Message) ([]error, bool) {
	var errs = make([]error, len(messages))
	var cmds = make([]*redis.Cmd, len(messages))
	pipe := pw.Client.Pipeline()

	for idx, message := range messages {
		// Reference the Payload in Body directly when writing to Redis ISB to avoid extra marshaling.
		// TODO: revisit directly Payload reference when Body structure changes
		cmds[idx] = script.Run(ctx, pipe, []string{pw.GetHashKeyName(message.EventTime), pw.Stream}, message.Header.ID, message.Header, message.Body.Payload, pw.PartitionWriteInfo.minId.String())
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
func (pw *RedisWriter) GetHashKeyName(startTime time.Time) string {
	return fmt.Sprintf("%s-h-%d", pw.Stream, startTime.Truncate(exactlyOnceHashWindow).Unix())
}

// GetStreamName gets the stream name. Stream name is derived from the name.
func (pw *RedisWriter) GetStreamName() string {
	return pw.Stream
}

// GetName gets the name of the Partition.
func (pw *RedisWriter) GetName() string {
	return pw.Name
}

func (pw *RedisWriter) GetPartitionIdx() int32 {
	return pw.PartitionIdx
}

// GetGroupName gets the name of the consumer group.
func (pw *RedisWriter) GetGroupName() string {
	return pw.Group
}

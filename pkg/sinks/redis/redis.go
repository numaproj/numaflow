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
	"github.com/go-redis/redis/v8"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"log"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// RedisSink is a sink to publish to redis. Currently, redis sink is mainly used for E2E testing.
type RedisSink struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
}

type Option func(sink *RedisSink) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(jss *RedisSink) error {
		jss.logger = log
		return nil
	}
}

// NewRedisSink returns RedisSink type.
func NewRedisSink(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*RedisSink, error) {
	bh := new(RedisSink)
	name := vertex.Spec.Name
	bh.name = name
	bh.pipelineName = vertex.Spec.PipelineName

	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	if bh.logger == nil {
		bh.logger = logging.NewLogger()
	}

	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSink), forward.WithLogger(bh.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	isdf, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: bh}, forward.All, applier.Terminal, fetchWatermark, publishWatermark, forwardOpts...)
	if err != nil {
		return nil, err
	}
	bh.isdf = isdf

	return bh, nil
}

// GetName returns the name.
func (rs *RedisSink) GetName() string {
	return rs.name
}

// IsFull returns whether sink is full.
func (rs *RedisSink) IsFull() bool {
	return false
}

// Write writes to the redis sink.
func (rs *RedisSink) Write(context context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	// TODO - redis options can be passed in from RedisSink attributes, as opposed to being hardcoded here.
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"redis-cluster:6379"},
	})

	// Our E2E tests time out after 20 minutes. Set redis message TTL to the same.
	const msgTTL = 20 * time.Minute

	// To serve various E2E test cases, the redis sink is written in a way that it constructs the key
	// as vertex name concatenated with message payload.
	// A dummy value 1 is set for every entry.
	for _, msg := range messages {
		key := fmt.Sprintf("%s-%s", rs.name, string(msg.Payload))
		err := client.Set(context, key, 1, msgTTL).Err()

		if err != nil {
			log.Println("Set Error - ", err)
		} else {
			log.Printf("Added key %s\n", key)
		}
	}

	sinkWriteCount.With(map[string]string{metricspkg.LabelVertex: rs.name, metricspkg.LabelPipeline: rs.pipelineName}).Add(float64(len(messages)))
	return nil, make([]error, len(messages))
}

func (rs *RedisSink) Close() error {
	return nil
}

// Start starts the sink.
func (rs *RedisSink) Start() <-chan struct{} {
	return rs.isdf.Start()
}

// Stop stops sinking
func (rs *RedisSink) Stop() {
	rs.isdf.Stop()
}

// ForceStop stops sinking
func (rs *RedisSink) ForceStop() {
	rs.isdf.ForceStop()
}

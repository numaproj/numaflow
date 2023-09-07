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

package udsink

import (
	"context"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type UserDefinedSink struct {
	name         string
	pipelineName string
	isdf         *sinkforward.DataForward
	logger       *zap.SugaredLogger
	udsink       SinkApplier
}

type Option func(*UserDefinedSink) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *UserDefinedSink) error {
		t.logger = log
		return nil
	}
}

// NewUserDefinedSink returns genericSink type.
func NewUserDefinedSink(vertex *dfv1.Vertex,
	fromBuffer isb.BufferReader,
	fetchWatermark fetch.Fetcher,
	publishWatermark publish.Publisher,
	idleManager wmb.IdleManagerInterface,
	udsink SinkApplier,
	opts ...Option) (*UserDefinedSink, error) {

	s := new(UserDefinedSink)
	name := vertex.Spec.Name
	s.name = name
	s.pipelineName = vertex.Spec.PipelineName
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}
	if s.logger == nil {
		s.logger = logging.NewLogger()
	}

	forwardOpts := []sinkforward.Option{sinkforward.WithLogger(s.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sinkforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	s.udsink = udsink
	isdf, err := sinkforward.NewDataForward(vertex, fromBuffer, s, fetchWatermark, publishWatermark, idleManager, forwardOpts...)
	if err != nil {
		return nil, err
	}
	s.isdf = isdf
	return s, nil
}

func (s *UserDefinedSink) GetName() string {
	return s.name
}

// GetPartitionIdx returns the partition index.
// for sink it is always 0.
func (s *UserDefinedSink) GetPartitionIdx() int32 {
	return 0
}

func (s *UserDefinedSink) IsFull() bool {
	return false
}

// Write writes to the UDSink container.
func (s *UserDefinedSink) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	msgs := make([]*sinkpb.SinkRequest, len(messages))
	for i, m := range messages {
		msgs[i] = &sinkpb.SinkRequest{
			Id:        m.ID,
			Value:     m.Payload,
			Keys:      m.Keys,
			EventTime: timestamppb.New(m.EventTime),
			// Watermark is only available in readmessage....
			Watermark: timestamppb.New(time.Time{}), // TODO: insert the correct watermark
		}
	}
	return nil, s.udsink.ApplySink(ctx, msgs)
}

func (s *UserDefinedSink) Close() error {
	return nil
}

func (s *UserDefinedSink) Start() <-chan struct{} {
	return s.isdf.Start()
}

func (s *UserDefinedSink) Stop() {
	s.isdf.Stop()
}

func (s *UserDefinedSink) ForceStop() {
	s.isdf.ForceStop()
}

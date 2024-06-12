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
)

type UserDefinedSink struct {
	name         string
	pipelineName string
	logger       *zap.SugaredLogger
	udsink       SinkApplier
}

func (s *UserDefinedSink) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	//TODO implement me
	panic("implement me")
}

// NewUserDefinedSink returns genericSink type.
func NewUserDefinedSink(ctx context.Context, vertexInstance *dfv1.VertexInstance, udsink SinkApplier) (*UserDefinedSink, error) {
	return &UserDefinedSink{
		name:         vertexInstance.Vertex.Spec.Name,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		logger:       logging.FromContext(ctx),
		udsink:       udsink,
	}, nil
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
			Headers:   m.Headers,
		}
	}
	return nil, s.udsink.ApplySink(ctx, msgs)
}

func (s *UserDefinedSink) Close() error {
	return nil
}

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

package blackhole

import (
	"context"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// Blackhole is a sink to emulate /dev/null
type Blackhole struct {
	name         string
	pipelineName string
	logger       *zap.SugaredLogger
}

func (b *Blackhole) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	//TODO implement me
	panic("implement me")
}

// NewBlackhole returns a new Blackhole sink.
func NewBlackhole(ctx context.Context, vertexInstance *dfv1.VertexInstance) (*Blackhole, error) {
	return &Blackhole{
		name:         vertexInstance.Vertex.Spec.Name,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		logger:       logging.FromContext(ctx),
	}, nil
}

// GetName returns the name.
func (b *Blackhole) GetName() string {
	return b.name
}

// GetPartitionIdx returns the partition index.
// for sink it is always 0.
func (b *Blackhole) GetPartitionIdx() int32 {
	return 0
}

// IsFull returns whether sink is full, which is never true.
func (b *Blackhole) IsFull() bool {
	// printing can never be full
	return false
}

// Write writes to the blackhole.
func (b *Blackhole) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	sinkWriteCount.With(map[string]string{metrics.LabelVertex: b.name, metrics.LabelPipeline: b.pipelineName}).Add(float64(len(messages)))

	return nil, make([]error, len(messages))
}

func (b *Blackhole) Close() error {
	return nil
}

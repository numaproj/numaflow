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

	forward2 "github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// Blackhole is a sink to emulate /dev/null
type Blackhole struct {
	name         string
	pipelineName string
	isdf         *forward2.InterStepDataForward
	logger       *zap.SugaredLogger
}

type Option func(*Blackhole) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(bl *Blackhole) error {
		bl.logger = log
		return nil
	}
}

// NewBlackhole returns Blackhole type.
func NewBlackhole(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*Blackhole, error) {
	bh := new(Blackhole)
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

	forwardOpts := []forward2.Option{forward2.WithVertexType(dfv1.VertexTypeSink), forward2.WithLogger(bh.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward2.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	isdf, err := forward2.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: bh}, forward2.All, applier.Terminal, fetchWatermark, publishWatermark, forwardOpts...)
	if err != nil {
		return nil, err
	}
	bh.isdf = isdf

	return bh, nil
}

// GetName returns the name.
func (b *Blackhole) GetName() string {
	return b.name
}

// IsFull returns whether sink is full, which is never true.
func (b *Blackhole) IsFull() bool {
	// printing can never be full
	return false
}

// Write writes to the blackhole.
func (b *Blackhole) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	sinkWriteCount.With(map[string]string{metricspkg.LabelVertex: b.name, metricspkg.LabelPipeline: b.pipelineName}).Add(float64(len(messages)))

	return nil, make([]error, len(messages))
}

func (b *Blackhole) Close() error {
	return nil
}

// Start starts the sink.
func (b *Blackhole) Start() <-chan struct{} {
	return b.isdf.Start()
}

// Stop stops sinking
func (b *Blackhole) Stop() {
	b.isdf.Stop()
}

// ForceStop stops sinking
func (b *Blackhole) ForceStop() {
	b.isdf.ForceStop()
}

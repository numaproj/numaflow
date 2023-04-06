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

package logger

import (
	"context"
	"log"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// ToLog prints the output to a log sinks.
type ToLog struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
}

type Option func(*ToLog) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *ToLog) error {
		t.logger = log
		return nil
	}
}

// NewToLog returns ToLog type.
func NewToLog(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*ToLog, error) {
	toLog := new(ToLog)
	name := vertex.Spec.Name
	toLog.name = name
	toLog.pipelineName = vertex.Spec.PipelineName
	// use opts in future for specifying logger format etc
	for _, o := range opts {
		if err := o(toLog); err != nil {
			return nil, err
		}
	}
	if toLog.logger == nil {
		toLog.logger = logging.NewLogger()
	}

	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSink), forward.WithLogger(toLog.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	isdf, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: toLog}, forward.All, applier.Terminal, fetchWatermark, publishWatermark, forwardOpts...)
	if err != nil {
		return nil, err
	}
	toLog.isdf = isdf

	return toLog, nil
}

// GetName returns the name.
func (t *ToLog) GetName() string {
	return t.name
}

// IsFull returns whether logging is full, which is never true.
func (t *ToLog) IsFull() bool {
	// printing can never be full
	return false
}

// Write writes to the log.
func (t *ToLog) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	prefix := "(" + t.GetName() + ")"
	for _, message := range messages {
		logSinkWriteCount.With(map[string]string{metrics.LabelVertex: t.name, metrics.LabelPipeline: t.pipelineName}).Inc()
		log.Println(prefix, " Payload - ", string(message.Payload), " Keys - ", message.Keys, " EventTime - ", message.EventTime.UnixMilli())
	}
	return nil, make([]error, len(messages))
}

func (t *ToLog) Close() error {
	return nil
}

// Start starts sinking to Log.
func (t *ToLog) Start() <-chan struct{} {
	return t.isdf.Start()
}

// Stop stops sinking
func (t *ToLog) Stop() {
	t.isdf.Stop()
}

// ForceStop stops sinking
func (t *ToLog) ForceStop() {
	t.isdf.ForceStop()
}

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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// ToLog prints the output to a log sinks.
type ToLog struct {
	name         string
	pipelineName string
	isdf         *sinkforward.DataForward
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
func NewToLog(vertexInstance *dfv1.VertexInstance,
	fromBuffer isb.BufferReader,
	fetchWatermark fetch.Fetcher,
	publishWatermark publish.Publisher,
	idleManager wmb.IdleManager,
	opts ...Option) (*ToLog, error) {

	toLog := new(ToLog)
	name := vertexInstance.Vertex.Spec.Name
	toLog.name = name
	toLog.pipelineName = vertexInstance.Vertex.Spec.PipelineName
	// use opts in future for specifying logger format etc
	for _, o := range opts {
		if err := o(toLog); err != nil {
			return nil, err
		}
	}
	if toLog.logger == nil {
		toLog.logger = logging.NewLogger()
	}

	forwardOpts := []sinkforward.Option{sinkforward.WithLogger(toLog.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sinkforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	isdf, err := sinkforward.NewDataForward(vertexInstance, fromBuffer, toLog, fetchWatermark, publishWatermark, idleManager, forwardOpts...)
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

// GetPartitionIdx returns the partition index.
// for sink it is always 0.
func (t *ToLog) GetPartitionIdx() int32 {
	return 0
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

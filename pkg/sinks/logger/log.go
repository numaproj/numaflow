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
	"fmt"
	"log"
	"strings"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// ToLog prints the output to a log sinks.
type ToLog struct {
	name         string
	pipelineName string
	logger       *zap.SugaredLogger
}

func (t *ToLog) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	//TODO implement me
	panic("implement me")
}

// NewToLog returns ToLog type.
func NewToLog(ctx context.Context, vertexInstance *dfv1.VertexInstance) (*ToLog, error) {
	return &ToLog{
		name:         vertexInstance.Vertex.Spec.Name,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		logger:       logging.FromContext(ctx),
	}, nil
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
		var hStr strings.Builder
		for k, v := range message.Headers {
			hStr.WriteString(fmt.Sprintf("%s: %s, ", k, v))
		}
		logSinkWriteCount.With(map[string]string{metrics.LabelVertex: t.name, metrics.LabelPipeline: t.pipelineName}).Inc()
		log.Println(prefix, " Payload - ", string(message.Payload), " Keys - ", message.Keys, " EventTime - ", message.EventTime.UnixMilli(), " Headers - ", hStr.String())
	}
	return nil, make([]error, len(messages))
}

func (t *ToLog) Close() error {
	return nil
}

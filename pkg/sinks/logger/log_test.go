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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestToLog_Start(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		AbstractVertex: dfv1.AbstractVertex{
			Name: "sinks.logger",
			Sink: &dfv1.Sink{
				Log: &dfv1.Log{},
			},
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex.Spec.Name})
	idleManager, _ := wmb.NewIdleManager(1, 1)
	s, err := NewToLog(vertexInstance, fromStep, fetchWatermark, publishWatermark[vertex.Spec.Name], idleManager)
	assert.NoError(t, err)

	stopped := s.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	s.Stop()

	<-stopped
}

// TestToLog_Forward writes to a vertex which has a logger sink
func TestToLog_Forward(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int64
	}{
		{
			name:      "batch_forward",
			batchSize: 1,
		},
		{
			name:      "batch_forward",
			batchSize: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchSize := tt.batchSize
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("sinks.logger1", 5*batchSize, 0)

			vertex1 := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				AbstractVertex: dfv1.AbstractVertex{
					Name: "sinks.logger1",
					Sink: &dfv1.Sink{
						Log: &dfv1.Log{},
					},
				},
			}}
			vertexInstance1 := &dfv1.VertexInstance{
				Vertex:  vertex1,
				Replica: 0,
			}

			fetchWatermark1, publishWatermark1 := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex1.Spec.Name})
			idleManager0, _ := wmb.NewIdleManager(1, 1)
			logger1, _ := NewToLog(vertexInstance1, to1, fetchWatermark1, publishWatermark1[vertex1.Spec.Name], idleManager0)
			logger1Stopped := logger1.Start()

			toSteps := map[string][]isb.BufferWriter{
				"sinks.logger1": {to1},
			}

			writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager1, _ := wmb.NewIdleManager(1, 1)
			f, err := sinkforward.NewDataForward(vertexInstance1, fromStep, to1, fetchWatermark, publishWatermark["sinks.logger1"], idleManager1, sinkforward.WithReadBatchSize(batchSize))
			assert.NoError(t, err)

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)
			f.Stop()
			<-stopped
			// downstream should be stopped only after upstream is stopped
			logger1.Stop()

			<-logger1Stopped
		})
	}
}

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

package forward

import (
	"context"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	"github.com/stretchr/testify/assert"
)

type myShutdownTest struct {
}

func (s myShutdownTest) WhereTo(_ []string, _ []string, _ string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{}, nil
}

func (s myShutdownTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (s myShutdownTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
}

func TestInterStepDataForward(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int64
		streamEnabled bool
	}{
		{
			name:          "stream_forward",
			batchSize:     1,
			streamEnabled: true,
		},
		{
			name:          "batch_forward",
			batchSize:     5,
			streamEnabled: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+"_stop", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startTime := time.Unix(1636470000, 0)
			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, startTime, nil)

			vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "testVertex",
				},
			}}

			vertexInstance := &dfv1.VertexInstance{
				Vertex:  vertex,
				Replica: 0,
			}

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myShutdownTest{}, myShutdownTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))
			assert.NoError(t, err)
			stopped := f.Start()
			// write some data but buffer is not full even though we are not reading
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)

			f.Stop()
			// we cannot assert the result of IsShuttingDown because it might take a couple of iterations to be successful.
			_, _ = f.IsShuttingDown()
			<-stopped
		})
		t.Run(tt.name+"_forceStop", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("to", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startTime := time.Unix(1636470000, 0)
			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, startTime, nil)

			vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "testVertex",
				},
			}}

			vertexInstance := &dfv1.VertexInstance{
				Vertex:  vertex,
				Replica: 0,
			}

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myShutdownTest{}, myShutdownTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))
			assert.NoError(t, err)
			stopped := f.Start()
			// write some data such that the fromBufferPartition can be empty, that is toBuffer gets full
			_, errs := fromStep.Write(ctx, writeMessages[0:4*batchSize])
			assert.Equal(t, make([]error, 4*batchSize), errs)

			f.Stop()
			canIShutdown, _ := f.IsShuttingDown()
			assert.Equal(t, true, canIShutdown)
			time.Sleep(1 * time.Millisecond)
			// only for canIShutdown will work as from buffer is not empty
			f.ForceStop()
			canIShutdown, err = f.IsShuttingDown()
			assert.NoError(t, err)
			assert.Equal(t, true, canIShutdown)

			<-stopped
		})
	}
}

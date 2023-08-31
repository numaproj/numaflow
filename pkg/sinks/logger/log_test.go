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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/stretchr/testify/assert"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	},
		{
			ToVertexName:         "to2",
			ToVertexPartitionIdx: 0,
		},
	}, nil
}

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
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex.Spec.Name})
	s, err := NewToLog(vertex, fromStep, fetchWatermark, publishWatermark, getSinkGoWhereDecider(vertex.Spec.Name))
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

// TestToLog_ForwardToTwoVertex writes to 2 vertices and have a logger sinks attached to each vertex.
func TestToLog_ForwardToTwoVertex(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			batchSize := tt.batchSize
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("to1", 5*batchSize, 0)
			to2 := simplebuffer.NewInMemoryBuffer("to2", 5*batchSize, 0)

			// start the last vertex first
			// add 2 sinks per vertex
			vertex1 := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				AbstractVertex: dfv1.AbstractVertex{
					Name: "sinks.logger1",
					Sink: &dfv1.Sink{
						Log: &dfv1.Log{},
					},
				},
			}}

			vertex2 := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				AbstractVertex: dfv1.AbstractVertex{
					Name: "sinks.logger2",
					Sink: &dfv1.Sink{
						Log: &dfv1.Log{},
					},
				},
			}}
			fetchWatermark1, publishWatermark1 := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex1.Spec.Name})
			logger1, _ := NewToLog(vertex1, to1, fetchWatermark1, publishWatermark1, getSinkGoWhereDecider(vertex1.Spec.Name))
			fetchWatermark2, publishWatermark2 := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex2.Spec.Name})
			logger2, _ := NewToLog(vertex2, to2, fetchWatermark2, publishWatermark2, getSinkGoWhereDecider(vertex2.Spec.Name))
			logger1Stopped := logger1.Start()
			logger2Stopped := logger2.Start()

			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
				"to2": {to2},
			}

			writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
			vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "testVertex",
				},
			}}
			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			f, err := sinkforward.NewDataForward(vertex, fromStep, toSteps, myForwardToAllTest{}, fetchWatermark, publishWatermark, sinkforward.WithReadBatchSize(batchSize))
			assert.NoError(t, err)

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)
			f.Stop()
			<-stopped
			// downstream should be stopped only after upstream is stopped
			logger1.Stop()
			logger2.Stop()

			<-logger1Stopped
			<-logger2Stopped
		})
	}
}

func getSinkGoWhereDecider(vertexName string) forward.GoWhere {
	fsd := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
		var result []forward.VertexBuffer
		result = append(result, forward.VertexBuffer{
			ToVertexName:         vertexName,
			ToVertexPartitionIdx: 0,
		})
		return result, nil
	})
	return fsd
}

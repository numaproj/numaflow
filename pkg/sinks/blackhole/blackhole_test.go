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
	"testing"
	"time"

	forward2 "github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/udf/applier"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestBlackhole_Start(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		AbstractVertex: dfv1.AbstractVertex{
			Name: "sinks.blackhole",
			Sink: &dfv1.Sink{
				Blackhole: &dfv1.Blackhole{},
			},
		},
	}}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertex.GetToBuffers()))
	s, err := NewBlackhole(vertex, fromStep, fetchWatermark, publishWatermark)
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

// TestBlackhole_ForwardToTwoVertex writes to 2 vertices and have a blackhole sinks attached to each vertex.
func TestBlackhole_ForwardToTwoVertex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 25)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 25)

	// start the last vertex first
	// add 2 sinks per vertex
	vertex1 := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		AbstractVertex: dfv1.AbstractVertex{
			Name: "sinks.blackhole1",
			Sink: &dfv1.Sink{
				Blackhole: &dfv1.Blackhole{},
			},
		},
	}}

	vertex2 := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		AbstractVertex: dfv1.AbstractVertex{
			Name: "sinks.blackhole2",
			Sink: &dfv1.Sink{
				Blackhole: &dfv1.Blackhole{},
			},
		},
	}}
	fetchWatermark1, publishWatermark1 := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertex1.GetToBuffers()))
	bh1, _ := NewBlackhole(vertex1, to1, fetchWatermark1, publishWatermark1)
	fetchWatermark2, publishWatermark2 := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertex2.GetToBuffers()))
	bh2, _ := NewBlackhole(vertex2, to2, fetchWatermark2, publishWatermark2)
	bh1Stopped := bh1.Start()
	bh2Stopped := bh2.Start()

	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
	}

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := forward2.NewInterStepDataForward(vertex, fromStep, toSteps, forward2.All, applier.Terminal, fetchWatermark, publishWatermark)
	assert.NoError(t, err)

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)
	f.Stop()
	<-stopped
	// downstream should be stopped only after upstream is stopped
	bh1.Stop()
	bh2.Stop()

	<-bh1Stopped
	<-bh2Stopped
}

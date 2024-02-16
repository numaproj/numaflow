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

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func TestBlackhole_Start(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
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
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex.Spec.Name})
	s, err := NewBlackhole(vertexInstance, fromStep, fetchWatermark, publishWatermark[vertex.Spec.Name], wmb.NewIdleManager(0, 1))
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

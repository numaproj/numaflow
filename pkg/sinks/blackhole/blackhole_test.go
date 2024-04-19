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
	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

func TestBlackhole_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime, nil)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		AbstractVertex: dfv1.AbstractVertex{
			Name: "sinks.blackhole",
			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					Blackhole: &dfv1.Blackhole{},
				},
			},
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	s, err := NewBlackhole(ctx, vertexInstance)
	assert.NoError(t, err)

	_, errs := s.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// write some data
	_, errs = s.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)
}

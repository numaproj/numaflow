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

package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "test",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func TestWithBufferSize(t *testing.T) {
	h := &httpSource{
		bufferSize: 10,
	}
	opt := WithBufferSize(100)
	assert.NoError(t, opt(h))
	assert.Equal(t, 100, h.bufferSize)
}

func TestWithReadTimeout(t *testing.T) {
	h := &httpSource{
		readTimeout: 4 * time.Second,
	}
	opt := WithReadTimeout(5 * time.Second)
	assert.NoError(t, opt(h))
	assert.Equal(t, 5*time.Second, h.readTimeout)
}

func Test_NewHTTP(t *testing.T) {
	v := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			AbstractVertex: dfv1.AbstractVertex{
				Name: "test-v",
				Source: &dfv1.Source{
					HTTP: &dfv1.HTTPSource{},
				},
			},
		},
	}
	vi := &dfv1.VertexInstance{
		Vertex:   v,
		Hostname: "test-host",
		Replica:  0,
	}
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}
	publishWMStores, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"test": publishWMStores,
	}

	idleManager, _ := wmb.NewIdleManager(1, len(toBuffers))
	h, err := New(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStores, idleManager)
	assert.NoError(t, err)
	assert.False(t, h.(*httpSource).ready)
	assert.Equal(t, v.Spec.Name, h.GetName())
	assert.NotNil(t, h.(*httpSource).forwarder)
	assert.NotNil(t, h.(*httpSource).shutdown)
	_ = h.Start()
	assert.True(t, h.(*httpSource).ready)
	h.Stop()
	assert.False(t, h.(*httpSource).ready)
}

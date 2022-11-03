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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"github.com/stretchr/testify/assert"
)

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
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
	publishWMStores := store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(map[string]isb.BufferWriter{})
	h, err := New(vi, dest, fetchWatermark, publishWatermark, publishWMStores)
	assert.NoError(t, err)
	assert.False(t, h.ready)
	assert.Equal(t, v.Spec.Name, h.GetName())
	assert.NotNil(t, h.forwarder)
	assert.NotNil(t, h.shutdown)
	_ = h.Start()
	assert.True(t, h.ready)
	h.Stop()
	assert.False(t, h.ready)
}

package http

import (
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
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
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
	h, err := New(v, dest)
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

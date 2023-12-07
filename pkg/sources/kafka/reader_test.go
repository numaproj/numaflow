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

package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func TestNewKafkasource(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{"b1"},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"testVertex": publishWMStore,
	}
	ks, err := NewKafkaSource(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)), WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	// no errors if everything is good.
	assert.Nil(t, err)
	assert.NotNil(t, ks)

	assert.Equal(t, "default", ks.(*kafkaSource).groupName)

	// config is all set and initialized correctly
	assert.NotNil(t, ks.(*kafkaSource).config)
	assert.Equal(t, 100, ks.(*kafkaSource).handlerBuffer)
	assert.Equal(t, 100*time.Millisecond, ks.(*kafkaSource).readTimeout)
	assert.Equal(t, 100, cap(ks.(*kafkaSource).handler.messages))
	assert.NotNil(t, ks.(*kafkaSource).forwarder)
}

func TestGroupNameOverride(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{"b1"}, ConsumerGroupName: "custom",
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"testVertex": publishWMStore,
	}
	ks, _ := NewKafkaSource(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)), WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, "default", ks.(*kafkaSource).groupName)

}

func TestDefaultBufferSize(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{"b1"},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"testVertex": publishWMStore,
	}
	ks, _ := NewKafkaSource(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)), WithLogger(logging.NewLogger()), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, 100, ks.(*kafkaSource).handlerBuffer)

}

func TestBufferSizeOverrides(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{"b1"},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"testVertex": publishWMStore,
	}
	ks, _ := NewKafkaSource(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)), WithLogger(logging.NewLogger()), WithBufferSize(110), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, 110, ks.(*kafkaSource).handlerBuffer)

}

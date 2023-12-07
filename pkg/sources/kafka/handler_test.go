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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "test",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func TestMessageHandling(t *testing.T) {

	topic := "testtopic"
	partition := int32(1)
	offset := int64(1)
	value := "testvalue"
	keys := []string{"testkey"}

	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Generator: nil,
				Kafka: &dfv1.KafkaSource{
					Topic: topic, Brokers: []string{"b1"},
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
		"test": publishWMStore,
	}
	ks, _ := NewKafkaSource(vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)), WithLogger(logging.NewLogger()),
		WithBufferSize(100), WithReadTimeOut(100*time.Millisecond))

	msg := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte(keys[0]),
		Value:     []byte(value),
	}

	expectedoffset := fmt.Sprintf("%s:%v:%v", topic, partition, offset)
	// push one message
	ks.(*kafkaSource).handler.messages <- msg

	readmsgs, err := ks.Read(context.Background(), 10)
	assert.Nil(t, err)
	assert.NotEmpty(t, readmsgs)

	assert.Equal(t, 1, len(readmsgs))

	readmsg := readmsgs[0]
	assert.Equal(t, expectedoffset, readmsg.ID)
	assert.Equal(t, []byte(value), readmsg.Body.Payload)
	assert.Equal(t, keys, readmsg.Header.Keys)
	assert.Equal(t, expectedoffset, readmsg.ReadOffset.String())
}

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

	mock "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func TestWriteSuccessToKafka(t *testing.T) {
	var err error
	fromStep := simplebuffer.NewInMemoryBuffer("toKafka", 25, 0)
	toKafka := new(ToKafka)
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Sink: &dfv1.Sink{
				Kafka: &dfv1.KafkaSink{},
			},
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertex.Spec.Name})
	toKafka.isdf, err = sinkforward.NewDataForward(vertexInstance, fromStep, toKafka, fetchWatermark, publishWatermark["testVertex"], wmb.NewIdleManager(0, 1))
	assert.NoError(t, err)
	toKafka.kafkaSink = vertex.Spec.Sink.Kafka
	toKafka.name = "Test"
	toKafka.topic = "topic-1"
	toKafka.log = logging.NewLogger()
	conf := mock.NewTestConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	producer := mock.NewAsyncProducer(t, conf)
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	toKafka.producer = producer
	toKafka.connected = true
	toKafka.Start()
	msgs := []isb.Message{
		{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{},
				Keys:        []string{"key1"},
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{},
				Keys:        []string{"key1"},
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
	}
	_, errs := toKafka.Write(context.Background(), msgs)
	for _, err := range errs {
		assert.Nil(t, err)
	}
	toKafka.Stop()
}

func TestWriteFailureToKafka(t *testing.T) {
	var err error
	fromStep := simplebuffer.NewInMemoryBuffer("toKafka", 25, 0)
	toKafka := new(ToKafka)
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Sink: &dfv1.Sink{
				Kafka: &dfv1.KafkaSink{},
			},
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	toSteps := map[string][]isb.BufferWriter{vertex.Spec.Name: {toKafka}}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	toKafka.isdf, err = sinkforward.NewDataForward(vertexInstance, fromStep, toKafka, fetchWatermark, publishWatermark["testVertex"], wmb.NewIdleManager(0, 1))
	assert.NoError(t, err)
	toKafka.name = "Test"
	toKafka.topic = "topic-1"
	toKafka.log = logging.NewLogger()
	conf := mock.NewTestConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	producer := mock.NewAsyncProducer(t, conf)
	producer.ExpectInputAndFail(fmt.Errorf("test"))
	producer.ExpectInputAndFail(fmt.Errorf("test1"))
	toKafka.producer = producer
	toKafka.connected = true
	toKafka.Start()
	msgs := []isb.Message{
		{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{},
				Keys:        []string{"key1"},
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{},
				Keys:        []string{"key1"},
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
	}
	_, errs := toKafka.Write(context.Background(), msgs)
	for _, err := range errs {
		assert.NotNil(t, err)
	}
	assert.Equal(t, "test", errs[0].Error())
	assert.Equal(t, "test1", errs[1].Error())
	toKafka.Stop()

}

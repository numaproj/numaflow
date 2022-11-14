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
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"testing"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	mock "github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestWriteSuccessToKafka(t *testing.T) {
	var err error
	fromStep := simplebuffer.NewInMemoryBuffer("toKafka", 25)
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
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertex.GetToBuffers()))
	toKafka.isdf, err = forward.NewInterStepDataForward(vertex, fromStep, map[string]isb.BufferWriter{"name": toKafka}, forward.All, applier.Terminal, fetchWatermark, publishWatermark)
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
				PaneInfo: isb.PaneInfo{},
				Key:      "key1",
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      "key1",
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
	fromStep := simplebuffer.NewInMemoryBuffer("toKafka", 25)
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
	toSteps := map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: toKafka}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	toKafka.isdf, err = forward.NewInterStepDataForward(vertex, fromStep, toSteps, forward.All, applier.Terminal, fetchWatermark, publishWatermark)
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
				PaneInfo: isb.PaneInfo{},
				Key:      "key1",
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      "key1",
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

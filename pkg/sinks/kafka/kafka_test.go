package kafka

import (
	"context"
	"fmt"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	mock "github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/isb/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
)

func TestWriteSuccessToKafka(t *testing.T) {
	var err error
	fromStep := simplebuffer.NewInMemoryBuffer("toKafka", 25)
	toKafka := new(ToKafka)
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	toKafka.isdf, err = forward.NewInterStepDataForward(vertex, fromStep, map[string]isb.BufferWriter{"name": toKafka}, forward.All, applier.Terminal, nil)
	assert.NoError(t, err)
	toKafka.name = "Test"
	toKafka.topic = "topic-1"
	toKafka.log = logging.NewLogger()
	toKafka.concurrency = 1
	conf := mock.NewTestConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	producer := mock.NewAsyncProducer(t, conf)
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	toKafka.producer = producer
	toKafka.Start()
	msgs := []isb.Message{
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      []byte("key1"),
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      []byte("key1"),
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
		},
	}}

	toKafka.isdf, err = forward.NewInterStepDataForward(vertex, fromStep, map[string]isb.BufferWriter{"name": toKafka}, forward.All, applier.Terminal, nil)
	assert.NoError(t, err)
	toKafka.name = "Test"
	toKafka.topic = "topic-1"
	toKafka.concurrency = 1
	toKafka.log = logging.NewLogger()
	conf := mock.NewTestConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	producer := mock.NewAsyncProducer(t, conf)
	producer.ExpectInputAndFail(fmt.Errorf("test"))
	producer.ExpectInputAndFail(fmt.Errorf("test1"))
	toKafka.producer = producer
	toKafka.Start()
	msgs := []isb.Message{
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      []byte("key1"),
			},
			Body: isb.Body{Payload: []byte("welcome1")},
		},
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{},
				Key:      []byte("key1"),
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

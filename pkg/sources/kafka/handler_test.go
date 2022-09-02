package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"github.com/stretchr/testify/assert"
)

func TestMessageHandling(t *testing.T) {

	topic := "testtopic"
	partition := int32(1)
	offset := int64(1)
	value := "testvalue"
	key := "testkey"

	tobuffer := simplebuffer.NewInMemoryBuffer("test", 100)
	dest := []isb.BufferWriter{tobuffer}

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
	publishWMStore := generic.BuildPublishWMStores(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(map[string]isb.BufferWriter{})
	ks, _ := NewKafkaSource(vi, dest, fetchWatermark, publishWatermark, publishWMStore, WithLogger(logging.NewLogger()),
		WithBufferSize(100), WithReadTimeOut(100*time.Millisecond))

	msg := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	expectedoffset := fmt.Sprintf("%s:%v:%v", topic, partition, offset)
	// push one message
	ks.handler.messages <- msg

	readmsgs, err := ks.Read(context.Background(), 10)
	assert.Nil(t, err)
	assert.NotEmpty(t, readmsgs)

	assert.Equal(t, 1, len(readmsgs))

	readmsg := readmsgs[0]
	assert.Equal(t, expectedoffset, readmsg.ID)
	assert.Equal(t, []byte(value), readmsg.Body.Payload)
	assert.Equal(t, key, readmsg.Header.Key)
	assert.Equal(t, expectedoffset, readmsg.ReadOffset.String())
}

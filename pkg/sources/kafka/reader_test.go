package kafka

import (
	"fmt"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/stretchr/testify/assert"
)

func TestNewKafkasource(t *testing.T) {
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
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
	ks, err := NewKafkaSource(vertex, dest, WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	// no errors if everything is good.
	assert.Nil(t, err)
	assert.NotNil(t, ks)

	assert.Equal(t, "default", ks.groupName)

	// config is all set and initialized correctly
	assert.NotNil(t, ks.config)
	assert.Equal(t, 100, ks.handlerbuffer)
	assert.Equal(t, 100*time.Millisecond, ks.readTimeout)
	assert.Equal(t, 100, cap(ks.handler.messages))
	assert.NotNil(t, ks.forwarder)
}

func TestGroupNameOverride(t *testing.T) {
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
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
	ks, _ := NewKafkaSource(vertex, dest, WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, "default", ks.groupName)

}

func TestDefaultBufferSize(t *testing.T) {
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
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
	ks, _ := NewKafkaSource(vertex, dest, WithLogger(logging.NewLogger()), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, 100, ks.handlerbuffer)

}

func TestBufferSizeOverrides(t *testing.T) {
	dest := []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("test", 100)}
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
	ks, _ := NewKafkaSource(vertex, dest, WithLogger(logging.NewLogger()), WithBufferSize(110), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, 110, ks.handlerbuffer)

}

func TestOffsetFrom(t *testing.T) {
	offstr := "t1:32:64"
	topic, partition, offset, err := offsetFrom(offstr)
	assert.Nil(t, err)
	assert.Equal(t, "t1", topic)
	assert.Equal(t, int32(32), partition)
	assert.Equal(t, int64(64), offset)
}

func TestToOffset(t *testing.T) {
	topic := "t1"
	partition := int32(1)
	offset := int64(23)

	formattedoffset := toOffset(topic, partition, offset)
	expected := fmt.Sprintf("%s:%v:%v", topic, partition, offset)
	assert.Equal(t, expected, formattedoffset)
}

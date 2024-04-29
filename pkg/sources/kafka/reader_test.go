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
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestNewKafkasource(t *testing.T) {
	// Create a new Sarama mock broker
	broker := sarama.NewMockBroker(t, 1)

	// Set expectations on the mock broker
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("testtopic", 0, broker.BrokerID()).
			SetController(broker.BrokerID()), // Set the controller
	})

	ctx := context.Background()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{broker.Addr()},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}

	handler := NewConsumerHandler(100)
	close(handler.ready)

	ks, err := NewKafkaSource(ctx, vi, handler, WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	// no errors if everything is good.
	assert.Nil(t, err)
	assert.NotNil(t, ks)

	assert.Equal(t, "default", ks.(*kafkaSource).groupName)

	// config is all set and initialized correctly
	assert.NotNil(t, ks.(*kafkaSource).config)
	assert.Equal(t, 100, ks.(*kafkaSource).handlerBuffer)
	assert.Equal(t, 100*time.Millisecond, ks.(*kafkaSource).readTimeout)
	assert.Equal(t, 100, cap(ks.(*kafkaSource).handler.messages))
}

func TestGroupNameOverride(t *testing.T) {
	// Create a new Sarama mock broker
	broker := sarama.NewMockBroker(t, 1)

	// Set expectations on the mock broker
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("testtopic", 0, broker.BrokerID()).
			SetController(broker.BrokerID()), // Set the controller
	})

	ctx := context.Background()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{broker.Addr()}, ConsumerGroupName: "custom",
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	handler := NewConsumerHandler(100)
	close(handler.ready)
	ks, _ := NewKafkaSource(ctx, vi, handler, WithLogger(logging.NewLogger()), WithBufferSize(100), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, "default", ks.(*kafkaSource).groupName)

}

func TestDefaultBufferSize(t *testing.T) {
	// Create a new Sarama mock broker
	broker := sarama.NewMockBroker(t, 1)

	// Set expectations on the mock broker
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("testtopic", 0, broker.BrokerID()).
			SetController(broker.BrokerID()), // Set the controller
	})

	ctx := context.Background()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{broker.Addr()},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	handler := NewConsumerHandler(100)
	close(handler.ready)
	ks, _ := NewKafkaSource(ctx, vi, handler, WithLogger(logging.NewLogger()), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))
	assert.Equal(t, 100, ks.(*kafkaSource).handlerBuffer)
	broker.Close()
}

func TestBufferSizeOverrides(t *testing.T) {
	// Create a new Sarama mock broker
	broker := sarama.NewMockBroker(t, 1)

	// Set expectations on the mock broker
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("testtopic", 0, broker.BrokerID()).
			SetController(broker.BrokerID()), // Set the controller
	})

	ctx := context.Background()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Source: &dfv1.Source{
				Kafka: &dfv1.KafkaSource{
					Topic: "testtopic", Brokers: []string{broker.Addr()},
				},
			},
		},
	}}
	vi := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "test-host",
		Replica:  0,
	}
	handler := NewConsumerHandler(100)
	close(handler.ready)
	ks, _ := NewKafkaSource(ctx, vi, handler, WithLogger(logging.NewLogger()), WithBufferSize(110), WithReadTimeOut(100*time.Millisecond), WithGroupName("default"))

	assert.Equal(t, 110, ks.(*kafkaSource).handlerBuffer)

}

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
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestKafkaSource_Read(t *testing.T) {
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

	// Create a new Sarama mock client
	client, err := sarama.NewClient([]string{broker.Addr()}, nil)
	assert.NoError(t, err)

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	assert.NoError(t, err)

	handler := NewConsumerHandler(100)

	ks := &kafkaSource{
		readTimeout:  1 * time.Second,
		vertexName:   vi.Vertex.Name,
		pipelineName: vi.Vertex.Spec.PipelineName,
		handler:      handler,
		logger:       logging.FromContext(ctx),
		saramaClient: client,
		adminClient:  adminClient,
	}

	// Write messages to the handler's messages channel
	go func() {
		for i := 0; i < 10; i++ {
			handler.messages <- &sarama.ConsumerMessage{
				Topic:     "testtopic",
				Partition: 0,
				Offset:    int64(i),
				Value:     []byte(fmt.Sprintf("message-%d", i)),
			}
		}
	}()

	// Test the Read method
	messages, err := ks.Read(context.Background(), 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 10)

	// Close the client and broker
	_ = client.Close()
	broker.Close()
}

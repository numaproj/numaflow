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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestMessageHandling(t *testing.T) {
	ctx := context.Background()
	topic := "testtopic"
	partition := int32(1)
	offset := fmt.Sprintf("%s:%d:%d", topic, partition, 1)
	value := "testvalue"
	keys := []string{"testkey"}

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

	ks := &kafkaSource{
		readTimeout:  3 * time.Second,
		vertexName:   vi.Vertex.Name,
		pipelineName: vi.Vertex.Spec.PipelineName,
		handler:      NewConsumerHandler(100),
		logger:       logging.FromContext(ctx),
	}

	msg := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    1,
		Key:       []byte(keys[0]),
		Value:     []byte(value),
	}

	expectedID := isb.MessageID{
		VertexName: vi.Vertex.Name,
		Offset:     fmt.Sprintf("%v", offset),
		Index:      partition,
	}
	// push one message
	ks.handler.messages <- msg

	readmsgs, err := ks.Read(context.Background(), 10)
	assert.Nil(t, err)
	assert.NotEmpty(t, readmsgs)

	assert.Equal(t, 1, len(readmsgs))

	readmsg := readmsgs[0]
	assert.Equal(t, expectedID, readmsg.ID)
	assert.Equal(t, []byte(value), readmsg.Body.Payload)
	assert.Equal(t, keys, readmsg.Header.Keys)
	assert.Equal(t, expectedID.Offset, readmsg.ReadOffset.String())
}

//go:build test

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

package kafka_e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete statefulset zookeeper kafka-broker --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/kafka -n numaflow-system
// Wait for zookeeper to come up
//go:generate sleep 60

type KafkaSuite struct {
	fixtures.E2ESuite
}

func (ks *KafkaSuite) TestKafkaSourceSink() {
	inputTopic := fixtures.GenerateKafkaTopicName()
	fixtures.CreateKafkaTopic(inputTopic, 1)

	outputTopic := fixtures.GenerateKafkaTopicName()
	fixtures.CreateKafkaTopic(outputTopic, 1)
	pipeline := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kafka-source-sink-e2e",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						Kafka: &dfv1.KafkaSource{
							Brokers:           []string{"kafka-broker:9092"},
							Topic:             inputTopic,
							ConsumerGroupName: "test-group",
						},
					},
				},
				{
					Name: "p1",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							Kafka: &dfv1.KafkaSink{
								Brokers: []string{"kafka-broker:9092"},
								Topic:   outputTopic,
							},
						},
					},
				},
			},
			Edges: []dfv1.Edge{
				{
					From: "input",
					To:   "p1",
				},
				{
					From: "p1",
					To:   "output",
				},
			},
		},
	}
	w := ks.Given().WithPipeline(pipeline).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	time.Sleep(30 * time.Second)
	fixtures.PumpKafkaTopic(inputTopic, 100, 20*time.Millisecond, 10)
	fixtures.ExpectKafkaTopicCount(outputTopic, 100, 20*time.Second)
	fixtures.DeleteKafkaTopic(outputTopic)
	fixtures.DeleteKafkaTopic(inputTopic)
}

func TestKafkaSuite(t *testing.T) {
	suite.Run(t, new(KafkaSuite))
}

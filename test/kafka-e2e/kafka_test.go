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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete statefulset zookeeper kafka-broker redpanda --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/kafka -n numaflow-system
//go:generate kubectl -n numaflow-system rollout status statefulset/redpanda --timeout=2m
//go:generate kubectl -n numaflow-system exec redpanda-0 -- rpk cluster health --watch --exit-when-healthy

type KafkaSuite struct {
	fixtures.E2ESuite
}

const (
	kafkaSourceVertex                = "input"
	kafkaRecoveryConfig              = "socket.timeout.ms: 3000\nsession.timeout.ms: 10000\nheartbeat.interval.ms: 3000\nreconnect.backoff.ms: 100"
	kafkaRebalanceLog                = "Pre rebalance Revoke"
	recoverableKafkaReadLog          = "Recoverable Kafka read error; returning partial or empty batch"
	fatalSourceForwarderLog          = "Error running pipeline"
	nonRetryableSourceAckLog         = "Non retryable error while invoking ack"
	kafkaRecoveryAssertionTimeout    = 90 * time.Second
	kafkaNegativeLogAssertionTimeout = 5 * time.Second
)

func kafkaSourceRedisPipeline(name, topic, consumerGroup, sinkHash string) *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: kafkaSourceVertex,
					Source: &dfv1.Source{
						Kafka: &dfv1.KafkaSource{
							Brokers:           []string{"kafka:9092"},
							Topic:             topic,
							ConsumerGroupName: consumerGroup,
							Config:            kafkaRecoveryConfig,
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							UDSink: &dfv1.UDSink{
								Container: &dfv1.Container{
									Image: "quay.io/numaio/numaflow-go/redis-sink:stable",
									Env: []corev1.EnvVar{
										{Name: "SINK_HASH_KEY", Value: sinkHash},
									},
								},
							},
						},
					},
				},
			},
			Edges: []dfv1.Edge{{From: kafkaSourceVertex, To: "output"}},
		},
	}
}

func (ks *KafkaSuite) createKafkaTopic(partitions int32) string {
	ks.T().Helper()
	topic := fixtures.GenerateKafkaTopicName()
	fixtures.CreateKafkaTopic(topic, partitions)
	ks.T().Cleanup(func() {
		fixtures.DeleteKafkaTopic(topic)
	})
	return topic
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
							Brokers:           []string{"kafka:9092"},
							Topic:             inputTopic,
							ConsumerGroupName: "test-group",
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							Kafka: &dfv1.KafkaSink{
								Brokers: []string{"kafka:9092"},
								Topic:   outputTopic,
							},
						},
					},
				},
			},
			Edges: []dfv1.Edge{
				{
					From: "input",
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

func (ks *KafkaSuite) TestKafkaRebalanceDoesNotRestartNuma() {
	const (
		partitions   = 4
		churnCycles  = 3
		messageCount = 2000
	)
	inputTopic := ks.createKafkaTopic(partitions)
	consumerGroup := fmt.Sprintf("kafka-rebalance-%s", inputTopic)
	sinkHash := fmt.Sprintf("kafka-rebalance-%s", inputTopic)

	pipelineA := kafkaSourceRedisPipeline("kafka-rebalance-a", inputTopic, consumerGroup, sinkHash)
	replicas := int32(1)
	pipelineA.Spec.Vertices[0].Scale = dfv1.Scale{Min: &replicas, Max: &replicas}
	bufferMaxLength := uint64(20)
	pipelineA.Spec.Vertices[1].Limits = &dfv1.VertexLimits{BufferMaxLength: &bufferMaxLength}
	wA := ks.Given().WithPipeline(pipelineA).When().CreatePipelineAndWait()
	defer wA.DeletePipelineAndWait()
	wA.Expect().VertexPodsRunning()
	snapshotA := wA.Expect().VertexPodRuntimeSnapshot(kafkaSourceVertex)
	ks.EqualValues(0, snapshotA.NumaRestartCount, "numa should not restart while starting pipeline A")

	for partition := 0; partition < partitions; partition++ {
		fixtures.SendMessage(inputTopic, fmt.Sprintf("warmup-%d", partition), fmt.Sprintf("warmup-%d", partition), partition)
	}
	wA.Expect().RedisSinkContains(sinkHash, "warmup-0")

	scaleOutput := func(replicas int) {
		ks.T().Helper()
		wA.Exec(
			"/bin/sh",
			[]string{
				"-c",
				fmt.Sprintf(
					"kubectl scale vtx %s-output --replicas=%d -n %s",
					pipelineA.Name,
					replicas,
					fixtures.Namespace,
				),
			},
			fixtures.CheckVertexScaled,
		)
		wA.Expect().VertexSizeScaledTo("output", replicas)
	}
	scaleOutput(0)

	pumpCtx, cancelPump := context.WithCancel(context.Background())
	pumpDone := fixtures.PumpKafkaTopicPartitionsAsync(
		pumpCtx,
		inputTopic,
		messageCount,
		partitions,
		time.Millisecond,
		10,
	)
	pumpFinished := false
	defer func() {
		cancelPump()
		if !pumpFinished {
			<-pumpDone
		}
	}()

	for cycle := 0; cycle < churnCycles; cycle++ {
		pipelineB := kafkaSourceRedisPipeline(
			fmt.Sprintf("kafka-rebalance-b-%d", cycle),
			inputTopic,
			consumerGroup,
			sinkHash,
		)
		wB := ks.Given().WithPipeline(pipelineB).When().CreatePipelineAndWait()
		wB.Expect().VertexPodsRunning()
		snapshotB := wB.Expect().VertexPodRuntimeSnapshot(kafkaSourceVertex)
		ks.EqualValues(0, snapshotB.NumaRestartCount, "numa should not restart while joining the consumer group")

		time.Sleep(2 * time.Second)
		wA.Expect().VertexNumaStable(snapshotA, kafkaSourceVertex)
		wB.Expect().VertexNumaStable(snapshotB, kafkaSourceVertex)
		wB.DeletePipelineAndWait()
	}

	scaleOutput(1)

	select {
	case err := <-pumpDone:
		pumpFinished = true
		ks.Require().NoError(err)
	case <-time.After(2 * time.Minute):
		ks.T().Fatal("timed out waiting for Kafka message pump")
	}

	wA.Expect().
		VertexPodLogContains(
			kafkaSourceVertex,
			kafkaRebalanceLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaRecoveryAssertionTimeout),
		).
		VertexNumaStable(snapshotA, kafkaSourceVertex)

	recoveryMarker := fmt.Sprintf("rebalance-recovered-%s", inputTopic)
	fixtures.SendMessage(inputTopic, recoveryMarker, recoveryMarker, 0)
	wA.Expect().
		RedisSinkContains(sinkHash, recoveryMarker).
		VertexNumaStable(snapshotA, kafkaSourceVertex).
		VertexPodLogNotContains(
			kafkaSourceVertex,
			fatalSourceForwarderLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaNegativeLogAssertionTimeout),
		).
		VertexPodLogNotContains(
			kafkaSourceVertex,
			nonRetryableSourceAckLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaNegativeLogAssertionTimeout),
		)
}

func (ks *KafkaSuite) TestKafkaBrokerInterruptionDoesNotRestartNuma() {
	inputTopic := ks.createKafkaTopic(1)
	consumerGroup := fmt.Sprintf("kafka-outage-%s", inputTopic)
	sinkHash := fmt.Sprintf("kafka-outage-%s", inputTopic)
	pipeline := kafkaSourceRedisPipeline("kafka-broker-interruption", inputTopic, consumerGroup, sinkHash)

	w := ks.Given().WithPipeline(pipeline).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	w.Expect().VertexPodsRunning()

	warmupMarker := fmt.Sprintf("broker-warmup-%s", inputTopic)
	fixtures.SendMessage(inputTopic, warmupMarker, warmupMarker, 0)
	w.Expect().RedisSinkContains(sinkHash, warmupMarker)

	snapshot := w.Expect().VertexPodRuntimeSnapshot(kafkaSourceVertex)
	ks.EqualValues(0, snapshot.NumaRestartCount, "numa should not restart before broker interruption")

	ks.Require().NoError(fixtures.RestartKafkaBroker(2 * time.Minute))
	fixtures.ResetKafkaClients()

	w.Expect().
		VertexPodLogContains(
			kafkaSourceVertex,
			recoverableKafkaReadLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaRecoveryAssertionTimeout),
		).
		VertexNumaStable(snapshot, kafkaSourceVertex)

	recoveryMarker := fmt.Sprintf("broker-recovered-%s", inputTopic)
	fixtures.SendMessage(inputTopic, recoveryMarker, recoveryMarker, 0)
	w.Expect().
		RedisSinkContains(sinkHash, recoveryMarker).
		VertexNumaStable(snapshot, kafkaSourceVertex).
		VertexPodLogNotContains(
			kafkaSourceVertex,
			fatalSourceForwarderLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaNegativeLogAssertionTimeout),
		).
		VertexPodLogNotContains(
			kafkaSourceVertex,
			nonRetryableSourceAckLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(kafkaNegativeLogAssertionTimeout),
		)
}

func TestKafkaSuite(t *testing.T) {
	suite.Run(t, new(KafkaSuite))
}

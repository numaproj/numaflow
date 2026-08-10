//go:build test

/*
Copyright 2026 The Numaproj Authors.

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

package pulsar_e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl delete -k ../../config/apps/pulsar -n numaflow-system --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/pulsar -n numaflow-system
//go:generate kubectl -n numaflow-system rollout status statefulset/pulsar --timeout=5m
//go:generate kubectl -n numaflow-system exec pulsar-0 -- curl --fail --silent --show-error http://127.0.0.1:8080/admin/v2/brokers/health

const (
	pulsarSourceVertex                = "input"
	pulsarAckPendingLog               = "Pulsar ack pending limit reached; returning empty batch"
	fatalSourceForwarderLog           = "Error running pipeline"
	nonRetryableSourceAckLog          = "Non retryable error while invoking ack"
	pulsarRecoveryAssertionTimeout    = 90 * time.Second
	pulsarNegativeLogAssertionTimeout = 5 * time.Second
)

type PulsarSuite struct {
	fixtures.E2ESuite
}

func pulsarSourceRedisPipeline(name, topic, subscription, sinkHash string, maxUnack uint32) *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: pulsarSourceVertex,
					Source: &dfv1.Source{
						Pulsar: &dfv1.PulsarSource{
							ServerAddr:       "pulsar://pulsar:6650",
							Topic:            topic,
							ConsumerName:     fmt.Sprintf("%s-consumer", name),
							SubscriptionName: subscription,
							MaxUnack:         maxUnack,
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
			Edges: []dfv1.Edge{{From: pulsarSourceVertex, To: "output"}},
		},
	}
}

func (ps *PulsarSuite) scaleOutput(w *fixtures.When, pipelineName string, replicas int) {
	ps.T().Helper()
	w.Exec(
		"/bin/sh",
		[]string{
			"-c",
			fmt.Sprintf(
				"kubectl scale vtx %s-output --replicas=%d -n %s",
				pipelineName,
				replicas,
				fixtures.Namespace,
			),
		},
		fixtures.CheckVertexScaled,
	)
	w.Expect().VertexSizeScaledTo("output", replicas)
}

func (ps *PulsarSuite) TestPulsarBrokerInterruptionDoesNotRestartNuma() {
	topic := fixtures.GeneratePulsarTopicName()
	subscription := fixtures.GeneratePulsarSubscriptionName()
	sinkHash := fmt.Sprintf("pulsar-restart-%s", subscription)
	pipeline := pulsarSourceRedisPipeline(
		"pulsar-broker-restart",
		topic,
		subscription,
		sinkHash,
		100,
	)

	w := ps.Given().WithPipeline(pipeline).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	w.Expect().VertexPodsRunning()

	warmupMarker := fmt.Sprintf("restart-warmup-%s", subscription)
	fixtures.SendPulsarMessage(topic, warmupMarker)
	w.Expect().RedisSinkContains(sinkHash, warmupMarker)

	snapshot := w.Expect().VertexPodRuntimeSnapshot(pulsarSourceVertex)
	ps.EqualValues(0, snapshot.NumaRestartCount, "numa should not restart before broker restart")

	ps.Require().NoError(fixtures.RestartPulsarBroker(3 * time.Minute))
	fixtures.ResetPulsarClients()

	recoveryMarker := fmt.Sprintf("restart-recovered-%s", subscription)
	fixtures.SendPulsarMessage(topic, recoveryMarker)
	w.Expect().
		RedisSinkContains(sinkHash, recoveryMarker).
		VertexNumaStable(snapshot, pulsarSourceVertex).
		VertexPodLogNotContains(
			pulsarSourceVertex,
			fatalSourceForwarderLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(pulsarNegativeLogAssertionTimeout),
		).
		VertexPodLogNotContains(
			pulsarSourceVertex,
			nonRetryableSourceAckLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(pulsarNegativeLogAssertionTimeout),
		)
}

func (ps *PulsarSuite) TestPulsarAckPendingDoesNotRestartNuma() {
	const maxUnack = 5

	topic := fixtures.GeneratePulsarTopicName()
	subscription := fixtures.GeneratePulsarSubscriptionName()
	sinkHash := fmt.Sprintf("pulsar-ack-pending-%s", subscription)
	pipeline := pulsarSourceRedisPipeline(
		"pulsar-ack-pending",
		topic,
		subscription,
		sinkHash,
		maxUnack,
	)
	readBatchSize := uint64(maxUnack)
	concurrency := uint64(maxUnack * 2)
	pipeline.Spec.Vertices[0].Limits = &dfv1.VertexLimits{
		ReadBatchSize: &readBatchSize,
		Concurrency:   &concurrency,
	}
	pipeline.Spec.Vertices[0].ContainerTemplate = &dfv1.ContainerTemplate{
		Env: []corev1.EnvVar{{Name: dfv1.EnvReadAhead, Value: "true"}},
	}
	bufferMaxLength := uint64(1)
	pipeline.Spec.Vertices[1].Limits = &dfv1.VertexLimits{BufferMaxLength: &bufferMaxLength}

	w := ps.Given().WithPipeline(pipeline).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	w.Expect().VertexPodsRunning()

	warmupMarker := fmt.Sprintf("ack-warmup-%s", subscription)
	fixtures.SendPulsarMessage(topic, warmupMarker)
	w.Expect().RedisSinkContains(sinkHash, warmupMarker)

	snapshot := w.Expect().VertexPodRuntimeSnapshot(pulsarSourceVertex)
	ps.EqualValues(0, snapshot.NumaRestartCount, "numa should not restart before ack-pending pressure")

	ps.scaleOutput(w, pipeline.Name, 0)
	fixtures.PumpPulsarTopic(topic, 100, time.Millisecond, "ack-pending", 10)

	w.Expect().
		VertexPodLogContains(
			pulsarSourceVertex,
			pulsarAckPendingLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(pulsarRecoveryAssertionTimeout),
		).
		VertexNumaStable(snapshot, pulsarSourceVertex)

	ps.scaleOutput(w, pipeline.Name, 1)
	recoveryMarker := fmt.Sprintf("ack-recovered-%s", subscription)
	fixtures.SendPulsarMessage(topic, recoveryMarker)
	w.Expect().
		RedisSinkContains(sinkHash, recoveryMarker).
		VertexNumaStable(snapshot, pulsarSourceVertex).
		VertexPodLogNotContains(
			pulsarSourceVertex,
			fatalSourceForwarderLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(pulsarNegativeLogAssertionTimeout),
		).
		VertexPodLogNotContains(
			pulsarSourceVertex,
			nonRetryableSourceAckLog,
			fixtures.PodLogCheckOptionWithContainer(dfv1.CtrMain),
			fixtures.PodLogCheckOptionWithTimeout(pulsarNegativeLogAssertionTimeout),
		)
}

func TestPulsarSuite(t *testing.T) {
	suite.Run(t, new(PulsarSuite))
}

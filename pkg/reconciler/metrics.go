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

package reconciler

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/numaproj/numaflow/pkg/metrics"
)

var (
	// BuildInfo provides the controller binary build information including version and platform, etc.
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "build_info",
		Help:      "A metric with a constant value '1', labeled with controller version and platform from which Numaflow was built",
	}, []string{metrics.LabelVersion, metrics.LabelPlatform})

	// ISBSvcHealth indicates whether the ISB Service is healthy (from k8s resource perspective).
	ISBSvcHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "isbsvc_health",
		Help:      "A metric to indicate whether the ISB Service is healthy. '1' means healthy, '0' means unhealthy",
	}, []string{metrics.LabelNamespace, metrics.LabelISBService})

	// PipelineHealth indicates whether the pipeline is healthy (from k8s resource perspective).
	PipelineHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "pipeline_health",
		Help:      "A metric to indicate whether the Pipeline is healthy. '1' means healthy, '0' means unhealthy",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline})

	PipelineDesiredPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "pipeline_desired_phase",
		Help:      "A metric to indicate the pipeline phase. '1' means Running, '2' means Paused",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline})

	PipelineCurrentPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "pipeline_current_phase",
		Help:      "A metric to indicate the pipeline phase. '0' means Unknown, '1' means Running, '2' means Paused, '3' means Failed, '4' means Pausing, '5' means 'Deleting'",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline})

	MonoVertexHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_health",
		Help:      "A metric to indicate whether the MonoVertex is healthy. '1' means healthy, '0' means unhealthy",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	MonoVertexDesiredPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_desired_phase",
		Help:      "A metric to indicate the MonoVertex phase. '1' means Running, '2' means Paused",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	MonoVertexCurrentPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_current_phase",
		Help:      "A metric to indicate the MonoVertex phase. '0' means Unknown, '1' means Running, '2' means Paused, '3' means Failed",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	// JetStreamISBSvcReplicas indicates the replicas of a JetStream ISB Service.
	JetStreamISBSvcReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "isbsvc_jetstream_replicas",
		Help:      "A metric indicates the replicas of a JetStream ISB Service",
	}, []string{metrics.LabelNamespace, metrics.LabelISBService})

	// VertexDesiredReplicas indicates the desired replicas of a Vertex.
	VertexDesiredReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "vertex_desired_replicas",
		Help:      "A metric indicates the desired replicas of a Vertex",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline, metrics.LabelVertex})

	// VertexCurrentReplicas indicates the current replicas of a Vertex.
	VertexCurrentReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "vertex_current_replicas",
		Help:      "A metric indicates the current replicas of a Vertex",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline, metrics.LabelVertex})

	// VertexMinReplicas indicates the min replicas of a Vertex.
	VertexMinReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "vertex_min_replicas",
		Help:      "A metric indicates the min replicas of a Vertex",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline, metrics.LabelVertex})

	// VertexMaxReplicas indicates the max replicas of a Vertex.
	VertexMaxReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "vertex_max_replicas",
		Help:      "A metric indicates the max replicas of a Vertex",
	}, []string{metrics.LabelNamespace, metrics.LabelPipeline, metrics.LabelVertex})

	// MonoVertexDesiredReplicas indicates the desired replicas of a MonoVertex.
	MonoVertexDesiredReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_desired_replicas",
		Help:      "A metric indicates the desired replicas of a MonoVertex",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	// MonoVertexCurrentReplicas indicates the current replicas of a MonoVertex.
	MonoVertexCurrentReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_current_replicas",
		Help:      "A metric indicates the current replicas of a MonoVertex",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	// MonoVertexMinReplicas indicates the min replicas of a MonoVertex.
	MonoVertexMinReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_min_replicas",
		Help:      "A metric indicates the min replicas of a MonoVertex",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})

	// MonoVertexMaxReplicas indicates the max replicas of a MonoVertex.
	MonoVertexMaxReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "monovtx_max_replicas",
		Help:      "A metric indicates the max replicas of a MonoVertex",
	}, []string{metrics.LabelNamespace, metrics.LabelMonoVertexName})
)

func init() {
	ctrlmetrics.Registry.MustRegister(BuildInfo, ISBSvcHealth, PipelineHealth,
		PipelineDesiredPhase, PipelineCurrentPhase, MonoVertexDesiredPhase, MonoVertexCurrentPhase,
		MonoVertexHealth, JetStreamISBSvcReplicas,
		VertexDesiredReplicas, VertexCurrentReplicas, VertexMinReplicas,
		VertexMaxReplicas, MonoVertexDesiredReplicas, MonoVertexCurrentReplicas,
		MonoVertexMinReplicas, MonoVertexMaxReplicas)
}

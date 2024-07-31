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
	}, []string{metrics.LabelNamespace, metrics.LabelISBService})

	// JetStreamISBSvcReplicas indicates the replicas of a JetStream ISB Service.
	JetStreamISBSvcReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "isbsvc_jetstream_replicas",
		Help:      "A metric indicates the replicas of a JetStream ISB Service",
	}, []string{metrics.LabelNamespace, metrics.LabelISBService})

	// RedisISBSvcReplicas indicates the replicas of a Redis ISB Service.
	RedisISBSvcReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "controller",
		Name:      "isbsvc_redis_replicas",
		Help:      "A metric indicates the replicas of a Redis ISB Service",
	}, []string{metrics.LabelNamespace, metrics.LabelISBService})

	// VertexDisiredReplicas indicates the desired replicas of a Vertex.
	VertexDisiredReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
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
)

func init() {
	ctrlmetrics.Registry.MustRegister(BuildInfo, ISBSvcHealth, PipelineHealth, JetStreamISBSvcReplicas, RedisISBSvcReplicas, VertexDisiredReplicas, VertexCurrentReplicas)
}

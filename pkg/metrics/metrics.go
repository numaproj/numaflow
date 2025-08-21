package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelVersion            = "version"
	LabelPlatform           = "platform"
	LabelNamespace          = "ns"
	LabelISBService         = "isbsvc"
	LabelPipeline           = "pipeline"
	LabelVertex             = "vertex"
	LabelVertexReplicaIndex = "replica"
	LabelVertexType         = "vertex_type"
	LabelPartitionName      = "partition_name"
	LabelMonoVertexName     = "mvtx_name"
	LabelComponent          = "component"
	LabelComponentName      = "component_name"
	LabelSDKLanguage        = "language"
	LabelSDKVersion         = "version"
	LabelSDKType            = "type" // container type, e.g sourcer, sourcetransformer, sinker, etc. see serverinfo.ContainerType
	LabelReason             = "reason"
	LabelPeriod             = "period"
)

var (
	// BuildInfo provides the Numaflow binary build information including version and platform, etc.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "build_info",
		Help: "A metric with a constant value '1', labeled by Numaflow binary version, platform, and other information",
	}, []string{LabelComponent, LabelComponentName, LabelVersion, LabelPlatform})

	// VertexLookBackSecs is a gauge used to indicate what is the current lookback window value being used by a given vertex.
	VertexLookBackSecs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "vertex",
		Name:      "lookback_window_seconds",
		Help: "A metric to show what is the lookback window value being used by a given vertex. " +
			"Look back Seconds is critical in autoscaling calculations",
	}, []string{LabelVertex, LabelVertexType})

	// Vertex Pending Messages is a gauge used to represent pending messages for a given vertex
	VertexPendingMessages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "vertex",
		Name:      "pending_messages",
		Help:      "A Gauge to keep track of the total number of pending messages for the vertex",
	}, []string{LabelPipeline, LabelVertex, LabelVertexType, LabelPartitionName, LabelPeriod})

	// MonoVertexLookBackSecs is a gauge used to indicate what is the current lookback window value being used
	// by a given monovertex. It is used as how many seconds to lookback for vertex average processing rate
	// (tps) and pending messages calculation, defaults to 120. Rate and pending messages metrics are
	// critical for autoscaling.
	MonoVertexLookBackSecs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "monovtx",
		Name:      "lookback_window_seconds",
		Help: "A metric to show what is the lookback window value being used by a given monovertex. " +
			"Look back Seconds is critical in autoscaling calculations",
	}, []string{LabelMonoVertexName})

	// MonoVertexPendingMessages is a gauge used to represent pending messages for a given monovertex
	MonoVertexPendingMessages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "monovtx",
		Name:      "pending",
		Help:      "A Gauge to keep track of the total number of pending messages for the monovtx",
	}, []string{LabelMonoVertexName, LabelPeriod})
)

var (
	// Pipeline processing lag, max(watermark) - min(watermark)
	PipelineProcessingLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "processing_lag",
		Help:      "Pipeline processing lag in milliseconds (max watermark - min watermark)",
	}, []string{LabelPipeline})

	// Now - max(watermark)
	WatermarkCmpNow = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "watermark_cmp_now",
		Help:      "Max source watermark compared with current time in milliseconds",
	}, []string{LabelPipeline})

	DataProcessingHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "data_processing_health",
		Help:      "Pipeline data processing health status. 1: Healthy, 0: Unknown, -1: Warning, -2: Critical",
	}, []string{LabelPipeline})
)

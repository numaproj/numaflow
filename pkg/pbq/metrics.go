package pbq

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// activePbqsCount is used to indicate the number of tasks in ordered processor
var activePbqsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "active_pbq_count",
	Help:      "Total number of PBQ's active",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

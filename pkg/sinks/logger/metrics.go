package logger

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// logSinkWriteCount is used to indicate the number of messages written to log sink
var logSinkWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "log_sink",
	Name:      "write_total",
	Help:      "Total number of messages written to log sink",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

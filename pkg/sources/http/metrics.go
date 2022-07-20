package http

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// httpSourceReadCount is used to indicate the number of messages read by the http source vertex
var httpSourceReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "http_source",
	Name:      "read_total",
	Help:      "Total number of messages Read",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

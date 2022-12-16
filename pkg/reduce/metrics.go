package reduce

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// readMessagesCount is used to indicate the number of messages read
var readMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_reader",
	Name:      "read_total",
	Help:      "Total number of Messages Read",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// readBytesCount is to indicate the number of bytes read
var readBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_reader",
	Name:      "read_bytes_total",
	Help:      "Total number of bytes read",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// readMessagesError is used to indicate the number of read errors
var readMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_reader",
	Name:      "read_error_total",
	Help:      "Total number of Read Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

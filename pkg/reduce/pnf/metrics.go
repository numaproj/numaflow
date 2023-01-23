package pnf

import (
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// writeMessagesCount is used to indicate the number of messages written
var writeMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, "buffer"})

// writeBytesCount is to indicate the number of bytes written
var writeBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_bytes_total",
	Help:      "Total number of bytes written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, "buffer"})

// writeMessagesError is used to indicate the number of errors messages written
var writeMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, "buffer"})

// platformError is used to indicate the number of Internal/Platform errors
var platformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "platform",
	Name:      "error_total",
	Help:      "Total number of platform Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// reduceProcessTime reduce task processing latency
var reduceProcessTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce_pnf",
	Name:      "process_time",
	Help:      "Reduce process time (1 to 1200000 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 1200000, 5),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// reduceForwardTime is used to indicate the time it took to forward the result
var reduceForwardTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce_pnf",
	Name:      "forward_time",
	Help:      "Reduce forward time (1 to 100000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 100000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

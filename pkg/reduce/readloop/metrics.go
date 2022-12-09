package readloop

import (
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelReason = "reason"
)

// droppedMessagesCount is used to indicate the number of messages dropped
var droppedMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce",
	Name:      "dropped_total",
	Help:      "Total number of Messages Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, LabelReason})

// pbqWriteErrorCount is used to indicate the number of errors while writing to pbq
var pbqWriteErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_error_total",
	Help:      "Total number of PBQ Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// pbqWriteMessagesCount is used to indicate the number of messages written to pbq
var pbqWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_total",
	Help:      "Total number of Messages Written to PBQ",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// orderedProcessorTaskCount is used to indicate the number of tasks in ordered processor
var orderedProcessorTaskCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce",
	Name:      "ordered_tasks_count",
	Help:      "Total number of Tasks in Ordered Processor",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// pbqWriteTime pbq write latency
var pbqWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_time",
	Help:      "Entry write time (1 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 5000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex})

// reduceProcessTime reduce task processing latency
var reduceProcessTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce",
	Name:      "reduce_process_time",
	Help:      "Lifespan of a pbq wal (1 to 1200 seconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 1200, 5),
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// ackMessagesCount is used to indicate the number of  messages acknowledged
var ackMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_forwarder",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// ackMessageError is used to indicate the errors in the number of  messages acknowledged
var ackMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_forwarder",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_forwarder",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

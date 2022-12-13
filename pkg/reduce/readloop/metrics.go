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
	Name:      "write_error_total",
	Help:      "Total number of PBQ Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// pbqWriteMessagesCount is used to indicate the number of messages written to pbq
var pbqWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "write_total",
	Help:      "Total number of Messages Written to PBQ",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// partitionsInFlight is used to indicate the partitions in flight
var partitionsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce",
	Name:      "partitions_inflight",
	Help:      "Total number of partitions in flight",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// pbqWriteTime pbq write latency
var pbqWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "write_time",
	Help:      "Entry write time (1 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 5000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex})

// ackMessagesCount is used to indicate the number of  messages acknowledged
var ackMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_reader",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// ackMessageError is used to indicate the errors in the number of  messages acknowledged
var ackMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_reader",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "udf",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

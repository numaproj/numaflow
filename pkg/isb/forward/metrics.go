package forward

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// readMessagesCount is used to indicate the number of messages read
var readMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_total",
	Help:      "Total number of Messages Read",
}, []string{"vertex", "pipeline", "buffer"})

// readMessagesError is used to indicate the number of errors messages read
var readMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_error_total",
	Help:      "Total number of Read Errors",
}, []string{"vertex", "pipeline", "buffer"})

// writeMessagesCount is used to indicate the number of messages written
var writeMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{"vertex", "pipeline", "buffer"})

// writeMessagesError is used to indicate the number of errors messages written
var writeMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{"vertex", "pipeline", "buffer"})

// ackMessagesCount is used to indicate the number of  messages acknowledged
var ackMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{"vertex", "pipeline", "buffer"})

// ackMessageError is used to indicate the errors in the number of  messages acknowledged
var ackMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{"vertex", "pipeline", "buffer"})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{"vertex", "pipeline", "buffer"})

// platformError is used to indicate the number of Internal/Platform errors
var platformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "platform_error_total",
	Help:      "Total number of platform Errors",
}, []string{"vertex", "pipeline"})

// forwardAChunkProcessingTime is a histogram to Observe forwardAChunk Processing times as a whole
var forwardAChunkProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "forward_chunk_processing_time",
	Help:      "Processing times of the entire forward a chunk (1 millisecond to 30 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(1000, 1000000*1800, 60),
}, []string{"vertex", "pipeline", "from", "to"})

// udfProcessingTime is a histogram to Observe UDF Processing times as a whole
var udfProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "udf_processing_time",
	Help:      "Processing times of UDF (100 microseconds to 15 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 1000000*900, 60),
}, []string{"vertex", "pipeline", "buffer"})

// concurrentUDFProcessingTime is a histogram to Observe UDF Processing times as a whole
var concurrentUDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "concurrent_udf_processing_time",
	Help:      "Processing times of Concurrent UDF (1 millisecond to 30 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(1000, 1000000*1800, 60),
}, []string{"vertex", "pipeline", "buffer"})

// udfReadMessagesCount is used to indicate the number of messages read by UDF
var udfReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_read_total",
	Help:      "Total number of Messages Read at UDF",
}, []string{"vertex", "pipeline", "buffer"})

// udfWriteMessagesCount is used to indicate the number of messages read by UDF
var udfWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_write_total",
	Help:      "Total number of Messages Written at UDF",
}, []string{"vertex", "pipeline", "buffer"})

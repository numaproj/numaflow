package metrics

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ReadMessagesCount is used to indicate the number of messages read
var ReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_total",
	Help:      "Total number of Messages Read",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// ReadBytesCount is to indicate the number of bytes read
var ReadBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_bytes_total",
	Help:      "Total number of bytes read",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// ReadMessagesError is used to indicate the number of errors messages read
var ReadMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_error_total",
	Help:      "Total number of Read Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// WriteMessagesCount is used to indicate the number of messages written
var WriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// WriteBytesCount is to indicate the number of bytes written
var WriteBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_bytes_total",
	Help:      "Total number of bytes written",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// WriteMessagesError is used to indicate the number of errors messages written
var WriteMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// AckMessagesCount is used to indicate the number of  messages acknowledged
var AckMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// AckMessageError is used to indicate the errors in the number of  messages acknowledged
var AckMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// UdfError is used to indicate the number of UDF errors
var UdfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// PlatformError is used to indicate the number of Internal/Platform errors
var PlatformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "platform_error_total",
	Help:      "Total number of platform Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// kafkaSinkWriteErrors is used to indicate the number of errors while while writing to kafka sink
var kafkaSinkWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_sink",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

// kafkaSinkWriteCount is used to indicate the number of messages written to kafka
var kafkaSinkWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_sink",
	Name:      "write_total",
	Help:      "Total number of errors on NewToKafka",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

var kafkaSinkWriteTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_sink",
	Name:      "write_timeout_total",
	Help:      "Total number of write timeouts on NewToKafka",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

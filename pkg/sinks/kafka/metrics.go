package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// kafkaSinkWriteErrors is used to indicate the number of errors while while writing to kafka sink
var kafkaSinkWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_sink",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{"vertex", "pipeline"})

// kafkaSinkWriteCount is used to indicate the number of messages written to kafka
var kafkaSinkWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_sink",
	Name:      "write_total",
	Help:      "Total number of errors on NewToKafka",
}, []string{"vertex", "pipeline"})

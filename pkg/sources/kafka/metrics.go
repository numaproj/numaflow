package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// kafkaSourceReadCount is used to indicate the number of messages read
var kafkaSourceReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_source",
	Name:      "read_total",
	Help:      "Total number of messages Read",
}, []string{"vertex", "pipeline"})

// kafkaSourceAckCount is used to indicate the number of messages Acknowledged
var kafkaSourceAckCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_source",
	Name:      "ack_total",
	Help:      "Total number of messages Acknowledged",
}, []string{"vertex", "pipeline"})

// kafkaSourceOffsetAckErrors is used to indicate the number of errors while reading from kafka source with offsets
var kafkaSourceOffsetAckErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "kafka_source",
	Name:      "ack_error_total",
	Help:      "Total number of Kafka ID Errors",
}, []string{"vertex", "pipeline"})

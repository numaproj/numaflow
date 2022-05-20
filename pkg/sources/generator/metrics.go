package generator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// tickgenSourceReadCount is used to indicate the number of messages read by tick generator
var tickgenSourceReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "tickgen_source",
	Name:      "read_total",
	Help:      "Total number of messages Read",
}, []string{"vertex", "pipeline"})

// tickgenSourceCount is used to indicate the number of times tickgen has ticked
var tickgenSourceCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "tickgen_source",
	Name:      "total",
	Help:      "Total number of times tickgen source has ticked",
}, []string{"vertex", "pipeline"})

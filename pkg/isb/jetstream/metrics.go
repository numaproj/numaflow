package jetstream

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// isbReadErrors is used to indicate the number of errors in the jetstream READ operations
var isbReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "read_error_total",
	Help:      "Total number of jetstream read errors",
}, []string{"buffer"})

// isbIsFullErrors is used to indicate the number of errors in the jetstream isFull check
var isbIsFullErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "isFull_error_total",
	Help:      "Total number of jetstream isFull errors",
}, []string{"buffer"})

// isbIsFull is used to indicate the counter for number of times buffer is full
var isbIsFull = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "isFull_total",
	Help:      "Total number of IsFull",
}, []string{"buffer"})

// isbWriteErrors is used to indicate the number of errors in the jetstream write check
var isbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "write_error_total",
	Help:      "Total number of jetstream write errors",
}, []string{"buffer"})

// isbBufferSoftUsage is used to indicate of buffer that is used up, it is calculated based on the messages in pending + ack pending
var isbBufferSoftUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_soft_usage",
	Help:      "percentage of buffer soft usage",
}, []string{"buffer"})

// isbBufferSolidUsage is used to indicate of buffer that is used up, it is calculated based on the messages remain in the stream (if it's not Limits retention policy)
var isbBufferSolidUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_solid_usage",
	Help:      "percentage of buffer solid usage",
}, []string{"buffer"})

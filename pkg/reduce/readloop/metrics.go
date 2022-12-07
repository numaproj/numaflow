package readloop

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// droppedMessagesCount is used to indicate the number of messages dropped
var droppedMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce",
	Name:      "dropped_total",
	Help:      "Total number of Messages Dropped",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, metricspkg.LabelReason})

// pbqWriteErrorCount is used to indicate the number of errors while writing to pbq
var pbqWriteErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_error_total",
	Help:      "Total number of PBQ Write Errors",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

// pbqWriteMessagesCount is used to indicate the number of messages written to pbq
var pbqWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_total",
	Help:      "Total number of Messages Written to PBQ",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

// orderedProcessorTaskCount is used to indicate the number of tasks in ordered processor
var orderedProcessorTaskCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce",
	Name:      "ordered_tasks_count",
	Help:      "Total number of Tasks in Ordered Processor",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

// pbqWriteTime pbq write latency
var pbqWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "pbq_write_time",
	Help:      "Entry write time (1 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 5000, 5),
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex})

// reduceProcessTime reduce task processing latency
var reduceProcessTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce",
	Name:      "reduce_process_time",
	Help:      "Lifespan of a pbq wal (1 to 1200 seconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 1200, 5),
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

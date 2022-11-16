package wal

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var FilesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_files_total",
	Help:      "Total number of wal files/partitions",
}, []string{metricspkg.LabelVertex})

var EntriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "total_entries_total",
	Help:      "Total number of entries written across ALL wal files/partitions",
}, []string{metricspkg.LabelVertex})

var GarbageCollectingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_garbage_collecting_time",
	Help:      "Garbage Collecting times of a pbq wal (100 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 5000, 5),
}, []string{metricspkg.LabelVertex, "pID"})

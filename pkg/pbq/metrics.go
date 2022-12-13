package pbq

import (
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// activePartitionCount is used to indicate the number of active partitions
var activePartitionCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "active_partition_count",
	Help:      "Total number of active partitions",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// pbqChannelSize is used to indicate the size of the pbq channel
var pbqChannelSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "channel_size",
	Help:      "PBQ Channel size (1 to 10000)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 10000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex})

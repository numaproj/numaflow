package pbq

import (
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// activePartitionCount is used to indicate the number of active partitions
var activePartitionCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce_pbq",
	Name:      "active_partition_count",
	Help:      "Total number of active partitions",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// pbqChannelSize is used to indicate the len of the pbq channel
var pbqChannelSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce_pbq",
	Name:      "channel_size",
	Help:      "PBQ Channel size",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

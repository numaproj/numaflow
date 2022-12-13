package pbq

import (
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// activePartitionCount is used to indicate the number of active partitions
var activePartitionCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "active_partition_count",
	Help:      "Total number of active partitions",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline})

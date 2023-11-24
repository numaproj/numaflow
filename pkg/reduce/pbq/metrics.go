/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

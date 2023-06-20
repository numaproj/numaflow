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

package pnf

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

// writeMessagesCount is used to indicate the number of messages written
var writeMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, metrics.LabelPartitionName})

// writeBytesCount is to indicate the number of bytes written
var writeBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_bytes_total",
	Help:      "Total number of bytes written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, metrics.LabelPartitionName})

// writeMessagesError is used to indicate the number of errors messages written
var writeMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, metrics.LabelPartitionName})

// dropMessagesCount is used to indicate the number of messages dropped
var dropMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "drop_total",
	Help:      "Total number of Messages Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, metrics.LabelPartitionName})

// dropBytesCount is used to indicate the number of bytes dropped
var dropBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_writer",
	Name:      "drop_bytes_total",
	Help:      "Total number of Bytes Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, metrics.LabelPartitionName})

// platformError is used to indicate the number of Internal/Platform errors
var platformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_pnf",
	Name:      "platform_error_total",
	Help:      "Total number of platform Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// reduceProcessTime reduce ForwardTask processing latency
var reduceProcessTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce_pnf",
	Name:      "process_time",
	Help:      "Reduce process time (1 to 1200000 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 1200000, 5),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// reduceForwardTime is used to indicate the time it took to forward the writeMessages
var reduceForwardTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce_pnf",
	Name:      "forward_time",
	Help:      "Reduce forward time (1 to 100000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 100000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

// partitionsInFlight is used to indicate the partitions in flight
var partitionsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce_pnf",
	Name:      "partitions_inflight",
	Help:      "Total number of partitions in flight",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_udf",
	Name:      "error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

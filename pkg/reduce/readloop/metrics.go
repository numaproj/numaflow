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

package readloop

import (
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelReason = "reason"
)

// droppedMessagesCount is used to indicate the number of messages dropped
var droppedMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_readloop",
	Name:      "dropped_total",
	Help:      "Total number of Messages Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex, LabelReason})

// pbqWriteErrorCount is used to indicate the number of errors while writing to pbq
var pbqWriteErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_pbq",
	Name:      "write_error_total",
	Help:      "Total number of PBQ Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// pbqWriteMessagesCount is used to indicate the number of messages written to pbq
var pbqWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_pbq",
	Name:      "write_total",
	Help:      "Total number of Messages Written to PBQ",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// partitionsInFlight is used to indicate the partitions in flight
var partitionsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "reduce_readloop",
	Name:      "partitions_inflight",
	Help:      "Total number of partitions in flight",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// pbqWriteTime pbq write latency
var pbqWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "reduce_pbq",
	Name:      "write_time",
	Help:      "Entry write time (1 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 5000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

// ackMessagesCount is used to indicate the number of  messages acknowledged
var ackMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_reader",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// ackMessageError is used to indicate the errors in the number of  messages acknowledged
var ackMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_isb_reader",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "reduce_udf",
	Name:      "error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelVersion            = "version"
	LabelPlatform           = "platform"
	LabelNamespace          = "ns"
	LabelISBService         = "isbsvc"
	LabelPipeline           = "pipeline"
	LabelVertex             = "vertex"
	LabelVertexReplicaIndex = "replica"
	LabelVertexType         = "vertex_type"
	LabelPartitionName      = "partition_name"

	LabelReason = "reason"
)

// Generic forwarder metrics
var (
	// ReadMessagesCount is used to indicate the number of total messages read
	ReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "read_total",
		Help:      "Total number of Messages Read",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// ReadDataMessagesCount is used to indicate the number of data messages read
	ReadDataMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "data_read_total",
		Help:      "Total number of Data Messages Read",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// ReadBytesCount is to indicate the number of bytes read
	ReadBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "read_bytes_total",
		Help:      "Total number of bytes read",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// ReadMessagesError is used to indicate the number of errors messages read
	ReadMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "read_error_total",
		Help:      "Total number of Read Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// WriteMessagesCount is used to indicate the number of messages written
	WriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "write_total",
		Help:      "Total number of Messages Written",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// WriteBytesCount is to indicate the number of bytes written
	WriteBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "write_bytes_total",
		Help:      "Total number of bytes written",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// WriteMessagesError is used to indicate the number of errors messages written
	WriteMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "write_error_total",
		Help:      "Total number of Write Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// DropMessagesCount is used to indicate the number of messages dropped
	DropMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "drop_total",
		Help:      "Total number of Messages Dropped",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName, LabelReason})

	// DropBytesCount is to indicate the number of bytes dropped
	DropBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "drop_bytes_total",
		Help:      "Total number of Bytes Dropped",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName, LabelReason})

	// AckMessagesCount is used to indicate the number of  messages acknowledged
	AckMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "ack_total",
		Help:      "Total number of Messages Acknowledged",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// AckMessageError is used to indicate the errors in the number of  messages acknowledged
	AckMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "ack_error_total",
		Help:      "Total number of Acknowledged Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// UDFError is used to indicate the number of UDF errors
	UDFError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "udf_error_total",
		Help:      "Total number of UDF Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex})

	// PlatformError is used to indicate the number of Internal/Platform errors
	PlatformError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "platform_error_total",
		Help:      "Total number of platform Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex})

	// ForwardAChunkProcessingTime is a histogram to Observe forwardAChunk Processing times as a whole
	ForwardAChunkProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "forwarder",
		Name:      "forward_chunk_processing_time",
		Help:      "Processing times of the entire forward a chunk (100 microseconds to 20 minutes)",
		Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 10),
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex})

	// UDFProcessingTime is a histogram to Observe UDF Processing times as a whole
	UDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "forwarder",
		Name:      "udf_processing_time",
		Help:      "Processing times of UDF (100 microseconds to 15 minutes)",
		Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*15, 10),
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex})

	// ConcurrentUDFProcessingTime is a histogram to Observe UDF Processing times as a whole
	ConcurrentUDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "forwarder",
		Name:      "concurrent_udf_processing_time",
		Help:      "Processing times of Concurrent UDF (100 microseconds to 20 minutes)",
		Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 10),
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex})

	// UDFReadMessagesCount is used to indicate the number of messages read by UDF
	UDFReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "udf_read_total",
		Help:      "Total number of Messages Read by UDF",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})

	// UDFWriteMessagesCount is used to indicate the number of messages written by UDF
	UDFWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "forwarder",
		Name:      "udf_write_total",
		Help:      "Total number of Messages Written by UDF",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})
)

// Source forwarder specific metrics
var (
	// SourceTransformerError is used to indicate the number of source transformer errors
	SourceTransformerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "source_forwarder",
		Name:      "transformer_error_total",
		Help:      "Total number of source transformer Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelPartitionName})

	// SourceTransformerProcessingTime is a histogram to Observe Source Transformer Processing times as a whole
	SourceTransformerProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "source_forwarder",
		Name:      "transformer_processing_time",
		Help:      "Processing times of source transformer (100 microseconds to 15 minutes)",
		Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*15, 10),
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelPartitionName})

	// SourceTransformerConcurrentProcessingTime is a histogram to Observe Source Transformer Processing times as a whole
	SourceTransformerConcurrentProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "source_forwarder",
		Name:      "concurrent_transformer_processing_time",
		Help:      "Processing times of Concurrent source transformer (100 microseconds to 20 minutes)",
		Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 10),
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelPartitionName})

	// SourceTransformerReadMessagesCount is used to indicate the number of messages read by source transformer
	SourceTransformerReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "source_forwarder",
		Name:      "transformer_read_total",
		Help:      "Total number of Messages Read by source transformer",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelPartitionName})

	// SourceTransformerWriteMessagesCount is used to indicate the number of messages written by source transformer
	SourceTransformerWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "source_forwarder",
		Name:      "transformer_write_total",
		Help:      "Total number of Messages Written by source transformer",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelPartitionName})
)

// Reduce forwarder specific metrics
var (
	// ReduceDroppedMessagesCount is used to indicate the number of messages dropped
	ReduceDroppedMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "reduce_data_forward",
		Name:      "dropped_total",
		Help:      "Total number of Messages Dropped",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex, LabelReason})

	// PBQWriteErrorCount is used to indicate the number of errors while writing to pbq
	PBQWriteErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "reduce_pbq",
		Name:      "write_error_total",
		Help:      "Total number of PBQ Write Errors",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})

	// PBQWriteMessagesCount is used to indicate the number of messages written to pbq
	PBQWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "reduce_pbq",
		Name:      "write_total",
		Help:      "Total number of Messages Written to PBQ",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})

	// PBQWriteTime pbq write latency
	PBQWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "reduce_pbq",
		Name:      "write_time",
		Help:      "Entry write time (1 to 5000 microseconds)",
		Buckets:   prometheus.ExponentialBucketsRange(1, 5000, 5),
	}, []string{LabelPipeline, LabelVertex, LabelVertexReplicaIndex})

	// ReduceProcessTime reduce ForwardTask processing latency
	ReduceProcessTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "reduce_pnf",
		Name:      "process_time",
		Help:      "Reduce process time (1 to 1200000 milliseconds)",
		Buckets:   prometheus.ExponentialBucketsRange(1, 1200000, 5),
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})

	// ReduceForwardTime is used to indicate the time it took to forward the writeMessages
	ReduceForwardTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "reduce_pnf",
		Name:      "forward_time",
		Help:      "Reduce forward time (1 to 100000 microseconds)",
		Buckets:   prometheus.ExponentialBucketsRange(1, 100000, 5),
	}, []string{LabelPipeline, LabelVertex, LabelVertexReplicaIndex})

	// ReducePartitionsInFlight is used to indicate the partitions in flight
	ReducePartitionsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "reduce_pnf",
		Name:      "partitions_inflight",
		Help:      "Total number of partitions in flight",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})

	// ActiveWindowsCount is used to indicate the number of active windows
	ActiveWindowsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "reduce",
		Name:      "active_windows",
		Help:      "Total number of active windows",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})

	// ClosedWindowsCount is used to indicate the number of closed windows
	ClosedWindowsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "reduce",
		Name:      "closed_windows",
		Help:      "Total number of closed windows",
	}, []string{LabelVertex, LabelPipeline, LabelVertexReplicaIndex})
)

// Ctrl Message Metric
var (
	// CtrlMessagesCount is used to indicate the number of total ctrl messages sent.
	CtrlMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "idlemanager",
		Name:      "ctrl_msg_total",
		Help:      "Total number of ctrl Messages sent",
	}, []string{LabelVertex, LabelPipeline, LabelVertexType, LabelVertexReplicaIndex, LabelPartitionName})
)

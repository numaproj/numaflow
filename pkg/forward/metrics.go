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

package forward

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

// ReadMessagesCount is used to indicate the number of messages read
var ReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_total",
	Help:      "Total number of Messages Read",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// ReadBytesCount is to indicate the number of bytes read
var ReadBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_bytes_total",
	Help:      "Total number of bytes read",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// ReadMessagesError is used to indicate the number of errors messages read
var ReadMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_error_total",
	Help:      "Total number of Read Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// WriteMessagesCount is used to indicate the number of messages written
var WriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// WriteBytesCount is to indicate the number of bytes written
var WriteBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_bytes_total",
	Help:      "Total number of bytes written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// WriteMessagesError is used to indicate the number of errors messages written
var WriteMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// DropMessagesCount is used to indicate the number of messages dropped
var DropMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "drop_total",
	Help:      "Total number of Messages Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// DropBytesCount is to indicate the number of bytes dropped
var DropBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "drop_bytes_total",
	Help:      "Total number of Bytes Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// AckMessagesCount is used to indicate the number of  messages acknowledged
var AckMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// AckMessageError is used to indicate the errors in the number of  messages acknowledged
var AckMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// UdfError is used to indicate the number of UDF errors
var UdfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// PlatformError is used to indicate the number of Internal/Platform errors
var PlatformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "platform_error_total",
	Help:      "Total number of platform Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// ForwardAChunkProcessingTime is a histogram to Observe forwardAChunk Processing times as a whole
var ForwardAChunkProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "forward_chunk_processing_time",
	Help:      "Processing times of the entire forward a chunk (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// UdfProcessingTime is a histogram to Observe UDF Processing times as a whole
var UdfProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "udf_processing_time",
	Help:      "Processing times of UDF (100 microseconds to 15 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*15, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// ConcurrentUDFProcessingTime is a histogram to Observe UDF Processing times as a whole
var ConcurrentUDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "concurrent_udf_processing_time",
	Help:      "Processing times of Concurrent UDF (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// UdfReadMessagesCount is used to indicate the number of messages read by UDF
var UdfReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_read_total",
	Help:      "Total number of Messages Read by UDF",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// UdfWriteMessagesCount is used to indicate the number of messages written by UDF
var UdfWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_write_total",
	Help:      "Total number of Messages Written by UDF",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

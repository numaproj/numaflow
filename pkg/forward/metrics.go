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

// readMessagesCount is used to indicate the number of messages read
var readMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_total",
	Help:      "Total number of Messages Read",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// readBytesCount is to indicate the number of bytes read
var readBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_bytes_total",
	Help:      "Total number of bytes read",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// readMessagesError is used to indicate the number of errors messages read
var readMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "read_error_total",
	Help:      "Total number of Read Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// writeMessagesCount is used to indicate the number of messages written
var writeMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_total",
	Help:      "Total number of Messages Written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// writeBytesCount is to indicate the number of bytes written
var writeBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_bytes_total",
	Help:      "Total number of bytes written",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// writeMessagesError is used to indicate the number of errors messages written
var writeMessagesError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "write_error_total",
	Help:      "Total number of Write Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// dropMessagesCount is used to indicate the number of messages dropped
var dropMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "drop_total",
	Help:      "Total number of Messages Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// dropBytesCount is to indicate the number of bytes dropped
var dropBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "drop_bytes_total",
	Help:      "Total number of Bytes Dropped",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// ackMessagesCount is used to indicate the number of  messages acknowledged
var ackMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_total",
	Help:      "Total number of Messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// ackMessageError is used to indicate the errors in the number of  messages acknowledged
var ackMessageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "ack_error_total",
	Help:      "Total number of Acknowledged Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// udfError is used to indicate the number of UDF errors
var udfError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_error_total",
	Help:      "Total number of UDF Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// platformError is used to indicate the number of Internal/Platform errors
var platformError = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "platform_error_total",
	Help:      "Total number of platform Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// forwardAChunkProcessingTime is a histogram to Observe forwardAChunk Processing times as a whole
var forwardAChunkProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "forward_chunk_processing_time",
	Help:      "Processing times of the entire forward a chunk (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// udfProcessingTime is a histogram to Observe UDF Processing times as a whole
var udfProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "udf_processing_time",
	Help:      "Processing times of UDF (100 microseconds to 15 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*15, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// concurrentUDFProcessingTime is a histogram to Observe UDF Processing times as a whole
var concurrentUDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "concurrent_udf_processing_time",
	Help:      "Processing times of Concurrent UDF (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// udfReadMessagesCount is used to indicate the number of messages read by UDF
var udfReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_read_total",
	Help:      "Total number of Messages Read by UDF",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

// udfWriteMessagesCount is used to indicate the number of messages written by UDF
var udfWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_write_total",
	Help:      "Total number of Messages Written by UDF",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelPartitionName})

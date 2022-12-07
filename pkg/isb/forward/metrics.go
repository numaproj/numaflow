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

	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// forwardAChunkProcessingTime is a histogram to Observe forwardAChunk Processing times as a whole
var forwardAChunkProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "forward_chunk_processing_time",
	Help:      "Processing times of the entire forward a chunk (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "from", "to"})

// udfProcessingTime is a histogram to Observe UDF Processing times as a whole
var udfProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "udf_processing_time",
	Help:      "Processing times of UDF (100 microseconds to 15 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*15, 60),
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// concurrentUDFProcessingTime is a histogram to Observe UDF Processing times as a whole
var concurrentUDFProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "forwarder",
	Name:      "concurrent_udf_processing_time",
	Help:      "Processing times of Concurrent UDF (100 microseconds to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 60000000*20, 60),
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// udfReadMessagesCount is used to indicate the number of messages read by UDF
var udfReadMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_read_total",
	Help:      "Total number of Messages Read at UDF",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

// udfWriteMessagesCount is used to indicate the number of messages read by UDF
var udfWriteMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "forwarder",
	Name:      "udf_write_total",
	Help:      "Total number of Messages Written at UDF",
}, []string{metricspkg.LabelVertex, metricspkg.LabelPipeline, "buffer"})

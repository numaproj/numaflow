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

package wal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

const (
	labelErrorKind = "kind"
)

// TODO - Adjust metric bucket range after we get more map reduce use cases.

var entriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entries_total",
	Help:      "Total number of entries written",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var entriesBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entries_bytes_total",
	Help:      "Total number of bytes written to WAL",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

var filesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_files_total",
	Help:      "Total number of wal files/partitions (including both active and closed)",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var activeFilesCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq_wal",
	Name:      "active_wal_files_total",
	Help:      "Total number of active wal files/partitions",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var garbageCollectingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_garbage_collecting_time",
	Help:      "Garbage Collecting time of a pbq wal (100 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 5000, 5),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var fileSyncWaitTime = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_file_sync_wait_time",
	Help:      "File Sync wait time",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var entryWriteLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entry_write_latency",
	Help:      "Entry write time to WAL",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var entryEncodeLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entry_encode_latency",
	Help:      "Time taken to encode an Entry",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var walErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_errors",
	Help:      "Errors encountered",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex, labelErrorKind})

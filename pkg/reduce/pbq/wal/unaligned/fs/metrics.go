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

package fs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

const (
	labelErrorKind = "kind"
)

// wal segment metrics
var segmentWALEntriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "unaligned_wal_entries_total",
	Help:      "Total number of entries written",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var segmentWALBytes = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "unaligned_wal_entries_bytes_total",
	Help:      "Total number of bytes written to unalignedWAL",
}, []string{metrics.LabelVertex, metrics.LabelPipeline, metrics.LabelVertexReplicaIndex})

var activeDataFilesCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "unaligned_wal_files_total",
	Help:      "Total number of active wal file both compacted and segment",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var segmentWALErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "unaligned_wal_errors",
	Help:      "Errors encountered",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex, labelErrorKind})

var segmentWALFileSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "unaligned_wal_file_size",
	Help:      "Size of the wal segment file",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

// gc events wal metrics
var gcWALEntriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "gc_wal_entries_total",
	Help:      "Total number of gc events written",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var gcWALBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "gc_wal_entries_bytes_total",
	Help:      "Total number of bytes written to gc events wal",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var gcWALFilesCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "gc_wal_files_total",
	Help:      "Total number of gc events wal files",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var gcWALErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "gc_wal_errors",
	Help:      "Errors encountered",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex, labelErrorKind})

var gcWALFileEventsCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "gc_wal_file_events_count",
	Help:      "Number of events in the gc events file",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

// compactor metrics
var compactionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "compactor_compaction_duration",
	Help:      "Duration of compaction",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var compactorErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq",
	Name:      "compactor_errors",
	Help:      "Errors encountered",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex, labelErrorKind})

var compactorFilesToCompact = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "compactor_files_to_compact",
	Help:      "Number of files to compact",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var compactorEventsToCompact = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq",
	Name:      "compactor_events_to_compact",
	Help:      "Number of events to compact",
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

var compactedFileSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq",
	Name:      "compactor_compacted_file_size",
	Help:      "Size of the compacted file",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
}, []string{metrics.LabelPipeline, metrics.LabelVertex, metrics.LabelVertexReplicaIndex})

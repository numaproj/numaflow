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
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	labelVertexReplicaIndex = "replica"
	labelErrorKind          = "kind"
)

// TODO - Adjust metric bucket range after we get more map reduce use cases.

var entriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entries_total",
	Help:      "Total number of entries written across ALL wal files/partitions",
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var filesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_files_total",
	Help:      "Total number of wal files/partitions (including both active and closed)",
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var activeFilesCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "pbq_wal",
	Name:      "active_wal_files_total",
	Help:      "Total number of active wal files/partitions",
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var garbageCollectingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_garbage_collecting_time",
	Help:      "Garbage Collecting time of a pbq wal (100 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 5000, 5),
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var fileSyncWaitTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_file_sync_wait_time",
	Help:      "File Sync wait time (1 to 60 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 60, 5),
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var entryWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entry_write_time",
	Help:      "Entry write time (1 to 60 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 60, 5),
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex})

var walErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_errors",
	Help:      "Errors encountered",
}, []string{metricspkg.LabelPipeline, metricspkg.LabelVertex, labelVertexReplicaIndex, labelErrorKind})

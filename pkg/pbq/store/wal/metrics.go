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
)

const (
	LabelPartitionKey = "partitionKey"
)

// TODO - Adjust metric bucket range after we get more map reduce use cases.

var entriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entries_total",
	Help:      "Total number of entries written across ALL wal files/partitions",
}, []string{})

var filesCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_files_total",
	Help:      "Total number of wal files/partitions",
}, []string{})

var garbageCollectingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_garbage_collecting_time",
	Help:      "Garbage Collecting time of a pbq wal (100 to 5000 microseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(100, 5000, 5),
}, []string{LabelPartitionKey})

var fileSyncWaitTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_file_sync_wait_time",
	Help:      "File Sync wait time (1 to 60 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 60, 5),
}, []string{LabelPartitionKey})

var entryWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_entry_write_time",
	Help:      "Entry write time (1 to 60 milliseconds)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 60, 5),
}, []string{LabelPartitionKey})

var lifespan = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "pbq_wal",
	Name:      "wal_lifespan",
	Help:      "Lifespan of a pbq wal (1 to 20 minutes)",
	Buckets:   prometheus.ExponentialBucketsRange(1, 20, 5),
}, []string{LabelPartitionKey})

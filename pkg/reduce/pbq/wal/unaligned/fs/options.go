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
	"time"
)

type WALOption func(stores *unalignedWAL)

// WithSegmentWALPath sets the segment WAL path
func WithSegmentWALPath(path string) WALOption {
	return func(stores *unalignedWAL) {
		stores.segmentWALPath = path
	}
}

// WithCompactWALPath sets the compact WAL path
func WithCompactWALPath(path string) WALOption {
	return func(stores *unalignedWAL) {
		stores.compactWALPath = path
	}
}

// WithSyncDuration sets the sync duration
func WithSyncDuration(maxDuration time.Duration) WALOption {
	return func(stores *unalignedWAL) {
		stores.syncDuration = maxDuration
	}
}

// WithMaxBatchSize sets the max batch size
func WithMaxBatchSize(size int64) WALOption {
	return func(stores *unalignedWAL) {
		stores.maxBatchSize = size
	}
}

// WithSegmentRotationDuration sets the segment rotation duration
func WithSegmentRotationDuration(maxDuration time.Duration) WALOption {
	return func(stores *unalignedWAL) {
		stores.segmentRotationDuration = maxDuration
	}
}

// WithSegmentSize sets the segment size
func WithSegmentSize(size int64) WALOption {
	return func(stores *unalignedWAL) {
		stores.segmentSize = size
	}
}

type GCEventsWALOption func(tracker *gcEventsWAL)

// WithGCTrackerRotationDuration sets the rotation duration for the GC events WAL
func WithGCTrackerRotationDuration(rotationDuration time.Duration) GCEventsWALOption {
	return func(tracker *gcEventsWAL) {
		tracker.rotationDuration = rotationDuration
	}
}

// WithEventsPath sets the path for the GC events WAL
func WithEventsPath(path string) GCEventsWALOption {
	return func(tracker *gcEventsWAL) {
		tracker.eventsPath = path
	}
}

// WithGCTrackerSyncDuration sets the sync duration for the GC events WAL
func WithGCTrackerSyncDuration(maxDuration time.Duration) GCEventsWALOption {
	return func(tracker *gcEventsWAL) {
		tracker.syncDuration = maxDuration
	}
}

// WithGCTrackerRotationEventsCount sets the rotation events count for the GC events WAL
func WithGCTrackerRotationEventsCount(count int) GCEventsWALOption {
	return func(tracker *gcEventsWAL) {
		tracker.rotationEventsCount = count
	}
}

type CompactorOption func(c *compactor)

// WithCompactorMaxFileSize sets the max file size for the compactor
func WithCompactorMaxFileSize(maxFileSize int64) CompactorOption {
	return func(c *compactor) {
		c.maxFileSize = maxFileSize
	}
}

// WithCompactorSyncDuration sets the sync duration for the compactor
func WithCompactorSyncDuration(maxDuration time.Duration) CompactorOption {
	return func(c *compactor) {
		c.syncDuration = maxDuration
	}
}

// WithCompactionDuration sets the compaction duration for the compactor
func WithCompactionDuration(maxDuration time.Duration) CompactorOption {
	return func(c *compactor) {
		c.compactionDuration = maxDuration
	}
}

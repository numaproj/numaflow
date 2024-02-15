package unaligned

import (
	"time"
)

type WALOption func(stores *unalignedWAL)

// WithStoreDataPath sets the store data path
func WithStoreDataPath(path string) WALOption {
	return func(stores *unalignedWAL) {
		stores.storeDataPath = path
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

type GCTrackerOption func(tracker *gcEventsTracker)

// WithGCTrackerRotationDuration sets the rotation duration for the GC tracker
func WithGCTrackerRotationDuration(rotationDuration time.Duration) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.rotationDuration = rotationDuration
	}
}

// WithEventsPath sets the path for the GC events
func WithEventsPath(path string) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.eventsPath = path
	}
}

// WithGCTrackerSyncDuration sets the sync duration for the GC tracker
func WithGCTrackerSyncDuration(maxDuration time.Duration) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.syncDuration = maxDuration
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

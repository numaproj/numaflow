package unaligned

import (
	"time"
)

type WALOption func(stores *WAL)

func WithStoreDataPath(path string) WALOption {
	return func(stores *WAL) {
		stores.storeDataPath = path
	}
}

func WithSyncDuration(maxDuration time.Duration) WALOption {
	return func(stores *WAL) {
		stores.syncDuration = maxDuration
	}
}

func WithMaxBatchSize(size int64) WALOption {
	return func(stores *WAL) {
		stores.maxBatchSize = size
	}
}

func WithSegmentRotationDuration(maxDuration time.Duration) WALOption {
	return func(stores *WAL) {
		stores.segmentRotationDuration = maxDuration
	}
}

func WithSegmentSize(size int64) WALOption {
	return func(stores *WAL) {
		stores.segmentSize = size
	}
}

type GCTrackerOption func(tracker *gcEventsTracker)

func WithGCTrackerRotationDuration(rotationDuration time.Duration) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.rotationDuration = rotationDuration
	}
}

func WithEventsPath(path string) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.eventsPath = path
	}
}

func WithGCTrackerSyncDuration(maxDuration time.Duration) GCTrackerOption {
	return func(tracker *gcEventsTracker) {
		tracker.syncDuration = maxDuration
	}
}

type CompactorOption func(c *compactor)

func WithCompactorMaxFileSize(maxFileSize int64) CompactorOption {
	return func(c *compactor) {
		c.maxFileSize = maxFileSize
	}
}

func WithCompactorSyncDuration(maxDuration time.Duration) CompactorOption {
	return func(c *compactor) {
		c.syncDuration = maxDuration
	}
}

func WithCompactionDuration(maxDuration time.Duration) CompactorOption {
	return func(c *compactor) {
		c.compactionDuration = maxDuration
	}
}

package unaligned

import (
	"time"
)

type StoreWriterOption func(stores *store)

func WithStoreDataPath(path string) StoreWriterOption {
	return func(stores *store) {
		stores.storeDataPath = path
	}
}

func WithSyncDuration(maxDuration time.Duration) StoreWriterOption {
	return func(stores *store) {
		stores.syncDuration = maxDuration
	}
}

func WithMaxBatchSize(size int64) StoreWriterOption {
	return func(stores *store) {
		stores.maxBatchSize = size
	}
}

func WithSegmentRotationDuration(maxDuration time.Duration) StoreWriterOption {
	return func(stores *store) {
		stores.segmentRotationDuration = maxDuration
	}
}

func WithSegmentSize(size int64) StoreWriterOption {
	return func(stores *store) {
		stores.segmentSize = size
	}
}

type GCTrackerOption func(tracker *gCEventsTracker)

func WithGCTrackerRotationDuration(rotationDuration time.Duration) GCTrackerOption {
	return func(tracker *gCEventsTracker) {
		tracker.rotationDuration = rotationDuration
	}
}

func WithEventsPath(path string) GCTrackerOption {
	return func(tracker *gCEventsTracker) {
		tracker.eventsPath = path
	}
}

func WithGCTrackerSyncDuration(maxDuration time.Duration) GCTrackerOption {
	return func(tracker *gCEventsTracker) {
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

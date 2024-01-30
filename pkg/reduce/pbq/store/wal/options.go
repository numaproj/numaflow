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

import "time"

type Option func(stores *walStores)

// WithStorePath sets the WAL store path
func WithStorePath(path string) Option {
	return func(stores *walStores) {
		stores.storePath = path
	}
}

// WithMaxBufferSize sets the WAL buffer max size option
func WithMaxBufferSize(size int64) Option {
	return func(stores *walStores) {
		stores.maxBatchSize = size
	}
}

// WithSyncDuration sets the WAL sync duration option
func WithSyncDuration(maxDuration time.Duration) Option {
	return func(stores *walStores) {
		stores.syncDuration = maxDuration
	}
}

type GCTrackerOption func(tracker *gCEventsTracker)

// WithGCTrackerMaxSize sets the WAL GC tracker max size option
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

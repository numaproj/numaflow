package unaligned

import (
	"os"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type options struct {
	storeDataPath           string
	storeOpPath             string
	segmentSize             int64
	syncDuration            time.Duration
	openMode                int
	segmentRotationDuration time.Duration
}

// DefaultOptions returns the default options
func DefaultOptions() *options {
	return &options{
		storeDataPath:           dfv1.DefaultStorePath,
		storeOpPath:             dfv1.DefaultStoreOpPath,
		segmentSize:             20 * 1024 * 1024,
		syncDuration:            dfv1.DefaultStoreSyncDuration,
		openMode:                os.O_WRONLY,
		segmentRotationDuration: 5 * time.Minute,
	}
}

// Option defines the function signature for options
type Option func(options *options)

// WithStoreDataPath sets the WAL store data path
func WithStoreDataPath(path string) Option {
	return func(options *options) {
		options.storeDataPath = path
	}
}

// WithStoreOpPath sets the WAL store op path
func WithStoreOpPath(path string) Option {
	return func(options *options) {
		options.storeOpPath = path
	}
}

// WithSegmentSize sets the WAL segment size
func WithSegmentSize(size int64) Option {
	return func(options *options) {
		options.segmentSize = size
	}
}

// WithSyncDuration sets the WAL sync duration option
func WithSyncDuration(maxDuration time.Duration) Option {
	return func(options *options) {
		options.syncDuration = maxDuration
	}
}

// WithSegmentRotationDuration sets the WAL segment rotation duration option
func WithSegmentRotationDuration(maxDuration time.Duration) Option {
	return func(options *options) {
		options.segmentRotationDuration = maxDuration
	}
}

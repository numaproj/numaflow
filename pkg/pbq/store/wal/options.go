package wal

import "time"

type options struct {
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration time.Duration
}

// Option is the write-ahead-log options.
type Option func(options *options) error

func defaultOptions() *options {
	return &options{
		maxBatchSize: 100,
		syncDuration: time.Second,
	}
}

// WithMaxBufferSize sets buffer max size option.
func WithMaxBufferSize(size int64) Option {
	return func(o *options) error {
		o.maxBatchSize = size
		return nil
	}
}

// WithSyncDuration sets sync duration option.
func WithSyncDuration(maxDuration time.Duration) Option {
	return func(o *options) error {
		o.syncDuration = maxDuration
		return nil
	}
}

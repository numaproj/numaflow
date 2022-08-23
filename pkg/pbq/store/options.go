package store

import dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

type Options struct {
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration int64
	// pbqStoreType store type (memory or s3 or file system)
	pbqStoreType string
	// ChannelSize buffered channel size
	BufferSize int64
	// StoreSize store array size
	StoreSize int64
}

func DefaultOptions() *Options {
	return &Options{
		maxBatchSize: dfv1.DefaultMaxBufferSize,
		syncDuration: dfv1.DefaultSyncDuration,
		pbqStoreType: dfv1.DefaultStoreType,
		BufferSize:   dfv1.DefaultBufferSize,
		StoreSize:    dfv1.DefaultStoreSize,
	}
}

type SetOption func(options *Options) error

// WithMaxBufferSize sets buffer max size option
func WithMaxBufferSize(size int64) SetOption {
	return func(o *Options) error {
		o.maxBatchSize = size
		return nil
	}
}

// WithSyncDuration sets sync duration option
func WithSyncDuration(maxDuration int64) SetOption {
	return func(o *Options) error {
		o.syncDuration = maxDuration
		return nil
	}
}

// WithPbqStoreType sets store type option
func WithPbqStoreType(storeType string) SetOption {
	return func(o *Options) error {
		o.pbqStoreType = storeType
		return nil
	}
}

// WithBufferSize sets buffer size option
func WithBufferSize(size int64) SetOption {
	return func(o *Options) error {
		o.BufferSize = size
		return nil
	}
}

// WithStoreSize sets store size option
func WithStoreSize(size int64) SetOption {
	return func(o *Options) error {
		o.StoreSize = size
		return nil
	}
}

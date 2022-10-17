package store

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type StoreOptions struct {
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration time.Duration
	// pbqStoreType store type (memory or s3 or file system)
	pbqStoreType dfv1.StoreType
	// storeSize store array size
	storeSize int64
}

func (o *StoreOptions) StoreSize() int64 {
	return o.storeSize
}

func (o *StoreOptions) PBQStoreType() dfv1.StoreType {
	return o.pbqStoreType
}

func DefaultOptions() *StoreOptions {
	return &StoreOptions{
		maxBatchSize: dfv1.DefaultStoreMaxBufferSize,
		syncDuration: dfv1.DefaultStoreSyncDuration,
		pbqStoreType: dfv1.DefaultStoreType,
		storeSize:    dfv1.DefaultStoreSize,
	}
}

type StoreOption func(options *StoreOptions) error

// WithMaxBufferSize sets buffer max size option
func WithMaxBufferSize(size int64) StoreOption {
	return func(o *StoreOptions) error {
		o.maxBatchSize = size
		return nil
	}
}

// WithSyncDuration sets sync duration option
func WithSyncDuration(maxDuration time.Duration) StoreOption {
	return func(o *StoreOptions) error {
		o.syncDuration = maxDuration
		return nil
	}
}

// WithPbqStoreType sets store type option
func WithPbqStoreType(storeType dfv1.StoreType) StoreOption {
	return func(o *StoreOptions) error {
		o.pbqStoreType = storeType
		return nil
	}
}

// WithStoreSize sets store size option
func WithStoreSize(size int64) StoreOption {
	return func(o *StoreOptions) error {
		o.storeSize = size
		return nil
	}
}

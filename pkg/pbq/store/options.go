package store

import dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

type Options struct {
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration int64
	// pbqStoreType store type (memory or s3 or file system)
	pbqStoreType dfv1.StoreType
	// ChannelSize buffered channel size
	bufferSize int64
	// storeSize store array size
	storeSize int64
	// readTimeoutSecs timeout in seconds for pbq reads
	readTimeoutSecs int
}

func (o *Options) PbqStoreType() dfv1.StoreType {
	return o.pbqStoreType
}

func (o *Options) StoreSize() int64 {
	return o.storeSize
}

func (o *Options) BufferSize() int64 {
	return o.bufferSize
}

func (o *Options) ReadTimeoutSecs() int {
	return o.readTimeoutSecs
}

func DefaultOptions() *Options {
	return &Options{
		maxBatchSize:    dfv1.DefaultPBQMaxBufferSize,
		syncDuration:    dfv1.DefaultPBQSyncDuration,
		pbqStoreType:    dfv1.DefaultPBQStoreType,
		bufferSize:      dfv1.DefaultPBQBufferSize,
		storeSize:       dfv1.DefaultPBQStoreSize,
		readTimeoutSecs: dfv1.DefaultPBQReadTimeoutSecs,
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
func WithPbqStoreType(storeType dfv1.StoreType) SetOption {
	return func(o *Options) error {
		o.pbqStoreType = storeType
		return nil
	}
}

// WithBufferSize sets buffer size option
func WithBufferSize(size int64) SetOption {
	return func(o *Options) error {
		o.bufferSize = size
		return nil
	}
}

// WithStoreSize sets store size option
func WithStoreSize(size int64) SetOption {
	return func(o *Options) error {
		o.storeSize = size
		return nil
	}
}

// WithReadTimeoutSecs sets read timeout option
func WithReadTimeoutSecs(seconds int) SetOption {
	return func(o *Options) error {
		o.readTimeoutSecs = seconds
		return nil
	}
}

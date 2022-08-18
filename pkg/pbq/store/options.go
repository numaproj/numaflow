package store

type StoreOptions struct {
	// maxBufferSize max size of buffer before it's flushed to store
	maxBufferSize int64
	// syncDuration timeout to sync to store
	syncDuration int64
	// pbqStoreType store type (s3 or file system)
	pbqStoreType string
	// ChannelSize buffered channel size
	ChannelSize int64
}

func DefaultPBQStoreOptions() *StoreOptions {
	return &StoreOptions{
		//maxBufferSize: dfv1.DefaultMaxBufferSize,
		//syncDuration:  dfv1.DefaultSyncDuration,
		//pbqStoreType:  dfv1.DefaultStoreType,
		//ChannelSize:   dfv1.DefaultChannelSize,
	}
}

type PbQStoreOption func(options *StoreOptions) error

// WithMaxBufferSize sets buffer max size option
func WithMaxBufferSize(size int64) PbQStoreOption {
	return func(o *StoreOptions) error {
		o.maxBufferSize = size
		return nil
	}
}

// WithSyncDuration sets sync duration option
func WithSyncDuration(maxDuration int64) PbQStoreOption {
	return func(o *StoreOptions) error {
		o.syncDuration = maxDuration
		return nil
	}
}

// WithPbqStoreType sets store type option
func WithPbqStoreType(storeType string) PbQStoreOption {
	return func(o *StoreOptions) error {
		o.pbqStoreType = storeType
		return nil
	}
}

func WithChannelSize(size int64) PbQStoreOption {
	return func(o *StoreOptions) error {
		o.ChannelSize = size
		return nil
	}
}

package pbq

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"time"
)

type options struct {
	// channelBufferSize buffered channel size
	channelBufferSize int64
	// readTimeout timeout in seconds for pbq reads
	readTimeout time.Duration
	// readBatchSize max size of batch to read from store
	readBatchSize int64
	// storeOptions options for pbq store
	storeOptions *store.StoreOptions
}

func (o *options) StoreOptions() *store.StoreOptions {
	return o.storeOptions
}

type PBQOption func(options *options) error

func DefaultOptions() *options {
	return &options{
		channelBufferSize: dfv1.DefaultPBQChannelBufferSize,
		readTimeout:       dfv1.DefaultPBQReadTimeout,
		readBatchSize:     dfv1.DefaultPBQReadBatchSize,
		storeOptions:      store.DefaultOptions(),
	}
}

// WithChannelBufferSize sets buffer size option
func WithChannelBufferSize(size int64) PBQOption {
	return func(o *options) error {
		o.channelBufferSize = size
		return nil
	}
}

// WithReadTimeout sets read timeout option
func WithReadTimeout(seconds time.Duration) PBQOption {
	return func(o *options) error {
		o.readTimeout = seconds
		return nil
	}
}

// WithReadBatchSize sets read batch size option
func WithReadBatchSize(size int64) PBQOption {
	return func(o *options) error {
		o.readBatchSize = size
		return nil
	}
}

// WithPBQStoreOptions sets different pbq store options
func WithPBQStoreOptions(opts ...store.StoreOption) PBQOption {
	return func(options *options) error {
		for _, opt := range opts {
			err := opt(options.storeOptions)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

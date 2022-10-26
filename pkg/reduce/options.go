package reduce

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/window"
)

// Options for forwarding the message
type Options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// windowOpts Options for window
	windowOpts *window.Options
}

type Option func(*Options) error

func DefaultOptions() *Options {
	return &Options{
		readBatchSize: dfv1.DefaultReadBatchSize,
		windowOpts:    window.DefaultOptions(),
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *Options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithWindowOptions sets different window options
func WithWindowOptions(opts ...window.Option) Option {
	return func(options *Options) error {
		for _, opt := range opts {
			err := opt(options.windowOpts)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

package window

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"time"
)

type Options struct {
	// windowType to specify the window type(fixed, sliding or session)
	windowType dfv1.WindowType
	// windowDuration to specify the duration of the window
	windowDuration time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		windowType:     dfv1.DefaultWindowType,
		windowDuration: dfv1.DefaultWindowDuration,
	}
}

type Option func(options *Options) error

// WithWindowType sets the window type
func WithWindowType(wt dfv1.WindowType) Option {
	return func(o *Options) error {
		o.windowType = wt
		return nil
	}
}

// WithWindowDuration sets the window duration
func WithWindowDuration(wd time.Duration) Option {
	return func(o *Options) error {
		o.windowDuration = wd
		return nil
	}
}

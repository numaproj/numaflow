package pnf

import (
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/window"
)

type options struct {
	gcEventsTracker unaligned.GCEventsTracker
	windowType      window.Type
}

type Option func(options *options) error

// WithGCEventsTracker sets the GCEventsTracker option
func WithGCEventsTracker(gcTracker unaligned.GCEventsTracker) Option {
	return func(o *options) error {
		o.gcEventsTracker = gcTracker
		return nil
	}
}

// WithWindowType sets the window type option
func WithWindowType(windowType window.Type) Option {
	return func(o *options) error {
		o.windowType = windowType
		return nil
	}
}

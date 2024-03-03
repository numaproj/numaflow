package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/window"
)

// noopCompactor is a no-op compactor which does not do any operation but can be safely invoked.
type noopCompactor struct {
}

// NewNoopCompactor returns a new no-op compactor
func NewNoopCompactor() unaligned.Compactor {
	return &noopCompactor{}
}

func (n noopCompactor) Start(ctx context.Context) error {
	return nil
}

func (n noopCompactor) Stop() error {
	return nil
}

// noopGCEventsWAL is a no-op gc events WAL which does not do any operation but can be safely invoked.
type noopGCEventsWAL struct {
}

// NewNoopGCEventsWAL returns a new no-op GCEventsWAL
func NewNoopGCEventsWAL() unaligned.GCEventsWAL {
	return &noopGCEventsWAL{}
}

func (n noopGCEventsWAL) PersistGCEvent(window window.TimedWindow) error {
	return nil
}

func (n noopGCEventsWAL) Close() error {
	return nil
}

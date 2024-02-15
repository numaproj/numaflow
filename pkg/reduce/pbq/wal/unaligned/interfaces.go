package unaligned

import (
	"context"

	"github.com/numaproj/numaflow/pkg/window"
)

// Compactor compacts the unalignedWAL by deleting the persisted messages
// which belongs to the materialized window.
type Compactor interface {
	// Start starts the compactor
	Start(ctx context.Context) error
	// Stop stops the compactor
	Stop() error
}

// GCEventsTracker tracks the GC events
type GCEventsTracker interface {
	TrackGCEvent(window window.TimedWindow) error
	Close() error
}

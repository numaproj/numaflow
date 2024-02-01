package unaligned

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/window"
)

type StoreWriter interface {
	// Write writes message to persistence store
	Write(msg *isb.ReadMessage) error
	// Close closes store
	Close() error
}

type StoreReader interface {
	// Read returns upto N(size) messages from the persisted store, it also returns
	// a boolean flag to indicate if the end of file has been reached.
	Read(size int64) ([]*isb.ReadMessage, bool, error)
}

// Compactor compacts the store by deleting the persisted messages
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

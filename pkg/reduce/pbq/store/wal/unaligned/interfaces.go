package unaligned

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/window"
)

type StoreWriter interface {
	// Write writes message to persistence store
	Write(msg *isb.ReadMessage) error
	// Compact deletes entries from the store for the given window
	Compact(window window.TimedWindow) error
	// Close closes store
	Close() error
}

type StoreReader interface {
	// Read returns upto N(size) messages from the persisted store, it also returns
	// a boolean flag to indicate if the end of file has been reached.
	Read(size int64) ([]*isb.ReadMessage, bool, error)
}

type Compactor interface {
	// Compact deletes entries from the store for the given window
	Compact(window window.TimedWindow) error
}

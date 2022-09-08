package store

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

// Store provides methods to read, write and delete data from the store.
type Store interface {
	// Read returns upto N(size) messages from the persisted store
	Read(size int64) ([]*isb.Message, bool, error)
	// Write writes message to persistence store
	Write(msg *isb.Message) error
	// Close closes store
	Close() error
	// GC does garbage collection and deletes all the messages that are persisted
	GC() error
}

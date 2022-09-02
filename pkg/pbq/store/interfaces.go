package store

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

// Store provides methods to read, write and delete data from a durable store.
type Store interface {
	// Read returns all the messages from the persisted store
	Read(size int64) ([]*isb.Message, bool, error)
	// Write writes message to persisted store
	Write(msg *isb.Message) error
	// Close closes store
	Close() error
	// GC does garbage collection and deletes all the messages that are persisted
	GC() error
	// IsEmpty checks if there are any records persisted in store
	IsEmpty() bool
}

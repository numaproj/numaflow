package store

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

// Store provides methods to read, write and delete data from a durable store.
type Store interface {
	//ReadFromStore returns all the messages from the persisted store
	ReadFromStore() ([]*isb.Message, error)
	//WriteToStore writes message to persisted store
	WriteToStore(msg *isb.Message) error
	// Close closes store
	Close() error
	// GC does garbage collection and deletes all the messages that are persisted
	GC() error
}

package memory

import (
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// MemoryStore implements PBQStore which stores the data in memory
type MemoryStore struct {
	closed   bool
	writePos int64
	storage  []*isb.Message
	options  *store.StoreOptions
}

//NewMemoryStore returns new memory store
func NewMemoryStore(opts ...store.PbQStoreOption) (*MemoryStore, error) {
	options := store.DefaultPBQStoreOptions()

	for _, opt := range opts {
		if opt != nil {
			if err := opt(options); err != nil {
				return nil, err
			}
		}
	}
	memStore := &MemoryStore{
		writePos: 0,
		closed:   false,
		storage:  make([]*isb.Message, options.StoreSize),
		options:  options,
	}

	return memStore, nil
}

// ReadFromStore will return all the messages that are persisted in store
// this function will be invoked during bootstrap if there is restart
func (m *MemoryStore) ReadFromStore() ([]*isb.Message, error) {
	if m.writePos == 0 {
		return nil, errors.New("no messages in store")
	}
	return m.storage[:m.writePos], nil
}

// WriteToStore writes message to store
func (m *MemoryStore) WriteToStore(msg *isb.Message) error {
	if m.writePos >= m.options.StoreSize {
		return errors.New("error while writing to store, store is full")
	}
	if m.closed {
		return errors.New("error while writing to store, store is closed")
	}
	m.storage[m.writePos] = msg
	m.writePos += 1
	return nil
}

// Close closes the store, no more writes to persistent store
// no implementation for in memory store
func (m *MemoryStore) Close() error {
	m.closed = true
	return nil
}

// GC does garbage collection
// for in-memory implementation we set the storage to nil, so that it will
// ready for GC
func (m *MemoryStore) GC() error {
	return nil
}

package memory

import (
	"errors"
	"fmt"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

var StoreFullError error = errors.New("error while writing to store, store is full")
var StoreClosedError error = errors.New("error while writing to store, store is closed")
var StoreEmptyError error = errors.New("error while reading from store, store is empty")

// MemoryStore implements PBQStore which stores the data in memory
type MemoryStore struct {
	closed   bool
	writePos int64
	readPos  int64
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
		readPos:  0,
		closed:   false,
		storage:  make([]*isb.Message, options.StoreSize),
		options:  options,
	}

	return memStore, nil
}

// ReadFromStore will return all the messages that are persisted in store
// this function will be invoked during bootstrap if there is restart
func (m *MemoryStore) ReadFromStore(size int64) ([]*isb.Message, error) {
	if m.writePos == 0 {
		return nil, StoreEmptyError
	}
	if m.readPos+size > m.writePos || m.readPos+size > int64(len(m.storage)) {
		return nil, fmt.Errorf("error while reading, request %d messages, but found only %d messages in store", size, m.writePos-m.readPos)
	}
	readMessages := make([]*isb.Message, size)
	copy(m.storage[m.readPos:m.readPos+size], readMessages)
	m.readPos += size
	return readMessages, nil
}

// WriteToStore writes message to store
func (m *MemoryStore) WriteToStore(msg *isb.Message) error {
	if m.writePos >= m.options.StoreSize {
		return StoreFullError
	}
	if m.closed {
		return StoreClosedError
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
	m.storage = nil
	m.writePos = -1
	return nil
}

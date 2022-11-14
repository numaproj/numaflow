/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memory

import (
	"context"
	"math"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
)

// memoryStore implements PBQStore which stores the data in memory
type memoryStore struct {
	closed      bool
	writePos    int64
	readPos     int64
	storage     []*isb.ReadMessage
	options     *store.StoreOptions
	log         *zap.SugaredLogger
	partitionID partition.ID
}

// NewMemoryStore returns new memory store
func NewMemoryStore(ctx context.Context, partitionID partition.ID, options *store.StoreOptions) (store.Store, error) {

	memStore := &memoryStore{
		writePos:    0,
		readPos:     0,
		closed:      false,
		storage:     make([]*isb.ReadMessage, options.StoreSize()),
		options:     options,
		log:         logging.FromContext(ctx).With("PBQ store", "Memory store").With("Partition ID", partitionID),
		partitionID: partitionID,
	}

	return memStore, nil
}

// ReadFromStore will return upto N messages persisted in store
// this function will be invoked during bootstrap if there is a restart
func (m *memoryStore) Read(size int64) ([]*isb.ReadMessage, bool, error) {
	if m.isEmpty() || m.readPos >= m.writePos {
		m.log.Errorw(store.ReadStoreEmptyErr.Error())
		return []*isb.ReadMessage{}, true, nil
	}

	// if size is greater than the number of messages in the store
	// we will assign size with the number of messages in the store
	size = int64(math.Min(float64(size), float64(m.writePos-m.readPos)))
	readMessages := m.storage[m.readPos : m.readPos+size]
	m.readPos += size
	return readMessages, false, nil
}

// WriteToStore writes a message to store
func (m *memoryStore) Write(msg *isb.ReadMessage) error {
	if m.writePos >= m.options.StoreSize() {
		m.log.Errorw(store.WriteStoreFullErr.Error(), zap.Any("msg header", msg.Header))
		return store.WriteStoreFullErr
	}
	if m.closed {
		m.log.Errorw(store.WriteStoreClosedErr.Error(), zap.Any("msg header", msg.Header))
		return store.WriteStoreClosedErr
	}
	m.storage[m.writePos] = msg
	m.writePos += 1
	return nil
}

// Close closes the store, no more writes to persistent store
// no implementation for in memory store
func (m *memoryStore) Close() error {
	m.closed = true
	return nil
}

// GC does garbage collection
// for in-memory implementation we set the storage to nil, so that it will
// ready for GC
func (m *memoryStore) GC() error {
	m.storage = nil
	m.writePos = -1
	return nil
}

// isEmpty check if there are any records persisted in store
func (m *memoryStore) isEmpty() bool {
	// is empty should return true when the store is created and no messages are written
	return m.writePos == 0
}

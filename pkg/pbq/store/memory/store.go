package memory

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/util"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
)

// MemoryStore implements PBQStore which stores the data in memory
type MemoryStore struct {
	closed      bool
	writePos    int64
	readPos     int64
	storage     []*isb.Message
	options     *store.Options
	log         *zap.SugaredLogger
	partitionID string
}

//NewMemoryStore returns new memory store
func NewMemoryStore(ctx context.Context, partitionID string, options *store.Options) (*MemoryStore, error) {

	memStore := &MemoryStore{
		writePos:    0,
		readPos:     0,
		closed:      false,
		storage:     make([]*isb.Message, options.StoreSize()),
		options:     options,
		log:         logging.FromContext(ctx).With("PBQ Store", "Memory Store").With("Partition ID", partitionID),
		partitionID: partitionID,
	}

	return memStore, nil
}

// ReadFromStore will return upto N messages persisted in store
// this function will be invoked during bootstrap if there is a restart
func (m *MemoryStore) ReadFromStore(size int64) ([]*isb.Message, error) {
	if m.IsEmpty() || m.readPos >= m.writePos {
		m.log.Errorw(store.ReadStoreEmptyError.Error())
		return []*isb.Message{}, store.ReadStoreEmptyError
	}

	size = util.Min(size, m.writePos-m.readPos)
	readMessages := m.storage[m.readPos : m.readPos+size]
	m.readPos += size
	return readMessages, nil
}

// WriteToStore writes message to store
func (m *MemoryStore) WriteToStore(msg *isb.Message) error {
	if m.writePos >= m.options.StoreSize() {
		m.log.Errorw(store.WriteStoreFullError.Error(), zap.Any("msg header", msg.Header))
		return store.WriteStoreFullError
	}
	if m.closed {
		m.log.Errorw(store.WriteStoreClosedError.Error(), zap.Any("msg header", msg.Header))
		return store.WriteStoreClosedError
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

// IsEmpty check if there are any records persisted in store
func (m *MemoryStore) IsEmpty() bool {
	if m.writePos == 0 {
		return true
	}
	return false
}

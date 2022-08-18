package simplepbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// MemoryStore implements PBQStore which stores the data in memory
type MemoryStore struct {
	input   chan *isb.Message
	output  chan *isb.Message
	storage []*isb.Message
	options *store.StoreOptions
}

func (m *MemoryStore) inputWatcher() {
	writePos := 0
	for msg := range m.input {
		m.storage[writePos] = msg
		writePos += 1
	}
}

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
		input:   make(chan *isb.Message, options.ChannelSize),
		output:  make(chan *isb.Message, options.ChannelSize),
		storage: make([]*isb.Message, options.ChannelSize),
		options: options,
	}

	go memStore.inputWatcher()

	return memStore, nil
}

func (m *MemoryStore) ReaderCh(ctx context.Context) chan *isb.Message {
	go m.fetchFromStorage(ctx)
	return m.output
}

func (m *MemoryStore) WriterCh() chan *isb.Message {
	return m.input
}

func (m *MemoryStore) CloseCh() {
	close(m.input)
}

func (m *MemoryStore) GC() error {
	return nil
}

func (m *MemoryStore) fetchFromStorage(ctx context.Context) {

ioLoop:
	for _, msg := range m.storage {
		select {
		case m.output <- msg:
		case <-ctx.Done():
			break ioLoop
		}
	}
	close(m.output)
}

package store

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Store provides methods to read, write and delete data from a durable store.
type Store interface {
	// ReaderCh exposes channel to read data from persistent store
	ReaderCh(ctx context.Context) chan *isb.Message
	// WriterCh exposes channel to write data to persistent store
	WriterCh() chan *isb.Message
	//CloseCh closes writer channel, no more writes
	CloseCh()
	// GC triggers garbage collection
	GC() error
}

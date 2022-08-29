package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Reader provides methods to read from PBQ.
type Reader interface {
	// ReadFromPBQ reads from PBQ
	ReadFromPBQ(ctx context.Context, size int64) ([]*isb.Message, error)
	// Close closes the reader.
	Close() error
	// GC does garbage collection, it deletes all the persisted data from the store
	GC() error
}

// Writer provides methods to write data to and close a PBQ.
// No data can be written to PBQ after cob.
type Writer interface {
	// WriteFromISB writes message to PBQ
	WriteFromISB(ctx context.Context, msg *isb.Message) error
	// CloseOfBook (cob) closes PBQ, no writes will be accepted after cob
	// Any pending data can be flushed to the persistent store at this point.
	CloseOfBook()
}

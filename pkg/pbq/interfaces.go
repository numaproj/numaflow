package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Reader provides methods to read from PBQ.
type Reader interface {
	// ReadFromPBQCh exposes channel to read from PBQ
	ReadFromPBQCh() <-chan *isb.Message
	// CloseReader closes the reader.
	CloseReader() error
	// GC does garbage collection, it deletes all the persisted data from the store
	GC() error
}

// Writer provides methods to write data to and close a PBQ.
// No data can be written to PBQ after cob.
type Writer interface {
	// WriteFromISB writes message to PBQ
	WriteFromISB(ctx context.Context, msg *isb.Message) error
	// CloseOfBook (cob) closes PBQ, no writes will be accepted after cob
	CloseOfBook()
	// CloseWriter to handle context close on writer
	// Any pending data can be flushed to the persistent store at this point.
	CloseWriter() error
}

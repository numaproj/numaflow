package pbq

import "github.com/numaproj/numaflow/pkg/isb"

// Reader provides methods to read from PBQ.
type Reader interface {
	// ReadFromPBQ exposes read channel to read from PBQ
	ReadFromPBQ() <-chan *isb.Message
	Close() error
	// GC does garbage collection, it deletes all the persisted data from the store
	GC() error
}

// Writer provides methods to write data to and close a PBQ.
// No data can be written to PBQ after it is closed.
type Writer interface {
	// WriteFromISB writes message to PBQ
	WriteFromISB(msg *isb.Message) error
	// CloseOfBook closes pbq, no writes will be accepted once closed
	CloseOfBook() error
}

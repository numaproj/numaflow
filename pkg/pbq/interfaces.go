package pbq

import (
	"context"
)

// Reader provides methods to read from PBQ.
type Reader interface {
	ReadFromPBQ(ctx context.Context) (<-chan interface{}, error)
	Close(ctx context.Context) error
	GC(ctx context.Context) error
}

// Writer provides methods to write data to and close a PBQ.
// No data can be written to PBQ after it is closed.
type Writer interface {
	WriteFromISB(ctx context.Context) (chan<- interface{}, error)
	CloseOfBook(ctx context.Context) error
}

type Serializable interface {
}

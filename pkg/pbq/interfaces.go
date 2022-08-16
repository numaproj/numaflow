package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
)

// PBQReader provides methods to read from PBQ.
type PBQReader interface {
	ReadCh(ctx context.Context) (<-chan *isb.Message, error)
	GC(ctx context.Context) error
}

// PBQWriter provides methods to write data to and close a PBQ.
// No data can be written to PBQ after it is closed.
type PBQWriter interface {
	WriteCh(ctx context.Context) (chan<- *isb.Message, error)
	CloseCh(ctx context.Context) error
}

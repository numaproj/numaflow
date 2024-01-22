package unaligned

import (
	"os"
	"path/filepath"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/window"
)

const SegmentPrefix = "segment"

type store struct {
	segmentId       int
	currDataFp      *os.File
	currOpFp        *os.File
	currWriteOffset int64
	currReadOffset  int64
	opts            *options
}

// NewStore returns a new store instance
func NewWriterStore(opts ...Option) (StoreWriter, error) {
	dOpts := DefaultOptions()
	for _, opt := range opts {
		opt(dOpts)
	}

	// we always start with segment 0
	dataFilePath := filepath.Join(dOpts.storeDataPath, SegmentPrefix+"-0")
	opFilePath := filepath.Join(dOpts.storeOpPath, SegmentPrefix+"-0")

	dataFp, err := os.OpenFile(dataFilePath, dOpts.openMode, 0644)
	if err != nil {
		return nil, err
	}

	opFp, err := os.OpenFile(opFilePath, dOpts.openMode, 0644)
	if err != nil {
		return nil, err
	}

	return &store{
		segmentId:       0,
		currDataFp:      dataFp,
		currOpFp:        opFp,
		currWriteOffset: 0,
		currReadOffset:  0,
		opts:            dOpts,
	}, nil

}

// Write writes message to persistence store
func (s *store) Write(msg *isb.ReadMessage) error {
	panic("not implemented") // TODO: Implement
}

// Compact deletes entries from the store for the given window
func (s *store) Compact(window window.TimedWindow) error {
	panic("not implemented") // TODO: Implement
}

// Close closes store
func (s *store) Close() error {
	panic("not implemented") // TODO: Implement
}

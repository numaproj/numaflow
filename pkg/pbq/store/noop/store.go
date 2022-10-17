package noop

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// PBQNoOpStore is a no-op pbq store which does not do any operation but can be safely invoked.
type PBQNoOpStore struct {
}

var _ store.Store = (*PBQNoOpStore)(nil)

func NewPBQNoOpStore() (*PBQNoOpStore, error) {
	return &PBQNoOpStore{}, nil
}

func (p *PBQNoOpStore) Read(size int64) ([]*isb.ReadMessage, bool, error) {
	return []*isb.ReadMessage{}, true, nil
}

func (p *PBQNoOpStore) Write(msg *isb.ReadMessage) error {
	return nil
}

func (p *PBQNoOpStore) Close() error {
	return nil
}

func (p *PBQNoOpStore) GC() error {
	return nil
}

package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbqstore"
)

type pbq struct {
	Store *pbqstore.Store
	out   chan [][]byte
}

func NewPBQ(ctx context.Context, store *pbqstore.Store) *pbq {
	return nil
}

func (p pbq) WriteCh(ctx context.Context) (chan<- *isb.Message, error) {
	//TODO implement me
	panic("implement me")
}

func (p pbq) CloseCh(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (p pbq) ReadCh(ctx context.Context) (<-chan *isb.Message, error) {
	//TODO implement me
	panic("implement me")
}

func (p pbq) GC(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

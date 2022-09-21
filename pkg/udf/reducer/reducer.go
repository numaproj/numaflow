package reducer

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Reducer applies the HTTPBasedUDF on the read message and gives back a new message. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type Reducer interface {
	Reduce(ctx context.Context, messageStream <-chan *isb.Message) ([]*isb.Message, error)
}

// ReduceFunc utility function used to create a Reducer implementation
type ReduceFunc func(context.Context, <-chan *isb.Message) ([]*isb.Message, error)

func (a ReduceFunc) Reduce(ctx context.Context, messageStream <-chan *isb.Message) ([]*isb.Message, error) {
	return a(ctx, messageStream)
}

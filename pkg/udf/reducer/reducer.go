package reducer

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
)

// Reducer applies the HTTPBasedUDF on the read message and gives back a new message. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type Reducer interface {
	Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error)
}

// ReduceFunc utility function used to create a Reducer implementation
type ReduceFunc func(context.Context, *partition.ID, <-chan *isb.ReadMessage) ([]*isb.Message, error)

func (a ReduceFunc) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	return a(ctx, partitionID, messageStream)
}

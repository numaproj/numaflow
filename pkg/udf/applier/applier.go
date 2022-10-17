package applier

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

// Applier applies the HTTPBasedUDF on the read message and gives back a new message. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type Applier interface {
	Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error)
}

// ApplyFunc utility function used to create an Applier implementation
type ApplyFunc func(context.Context, *isb.ReadMessage) ([]*isb.Message, error)

func (a ApplyFunc) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return a(ctx, message)
}

var (
	// Terminal Applier do not make any change to the message
	Terminal = ApplyFunc(func(ctx context.Context, msg *isb.ReadMessage) ([]*isb.Message, error) {
		return []*isb.Message{&msg.Message}, nil
	})
)

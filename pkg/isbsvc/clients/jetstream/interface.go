package jetstream

import "context"

// JetStreamClient is used to provide a jetstream client
type JetStreamClient interface {
	Connect(ctx context.Context, opts ...JetStreamClientOption) (*NatsConn, error)
}

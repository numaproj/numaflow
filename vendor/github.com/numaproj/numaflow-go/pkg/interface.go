package numaflow

import "context"

// Server is the interface for the all the numaflow servers.
type Server interface {
	// Start starts the server.
	Start(ctx context.Context) error
}

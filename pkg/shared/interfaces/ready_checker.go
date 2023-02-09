package interfaces

import "context"

// ReadyChecker is the interface to check if the server is ready
type ReadyChecker interface {
	// WaitUntilReady waits for the server to get ready
	WaitUntilReady(ctx context.Context) error
}

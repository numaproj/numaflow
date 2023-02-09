package interfaces

import "context"

// ReadyChecker is the interface to check if the user defined container is ready
type ReadyChecker interface {
	// WaitUntilReady waits for the user defined container to get ready
	WaitUntilReady(ctx context.Context) error
}

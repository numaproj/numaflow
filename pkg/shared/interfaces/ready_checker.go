package interfaces

import "context"

// ConnectionChecker is the interface to check if the user defined container is connected and ready to use
type ConnectionChecker interface {
	// WaitUntilReady waits for the user defined container to get ready
	WaitUntilReady(ctx context.Context) error
}

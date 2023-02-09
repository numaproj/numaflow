package interfaces

import "context"

type ReadyChecker interface {
	WaitUntilReady(ctx context.Context) error
}

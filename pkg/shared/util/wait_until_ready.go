package util

import "context"

type ReadyChecker interface {
	WaitUntilReady(ctx context.Context) error
}

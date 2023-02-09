package util

import "context"

type ServerHandler interface {
	WaitUntilReady(ctx context.Context) error
}

package cat

import (
	"context"

	funcsdk "github.com/numaproj/numaflow/sdks/golang/function"
)

func New() funcsdk.Handle {
	return func(ctx context.Context, key, msg []byte) (funcsdk.Messages, error) {
		return funcsdk.MessagesBuilder().Append(funcsdk.MessageToAll(msg)), nil
	}
}

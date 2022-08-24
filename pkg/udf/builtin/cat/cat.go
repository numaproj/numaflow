package cat

import (
	"context"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
)

func New() functionsdk.DoFunc {
	return func(ctx context.Context, key string, msg []byte) (functionsdk.Messages, error) {
		return functionsdk.MessagesBuilder().Append(functionsdk.MessageToAll(msg)), nil
	}
}

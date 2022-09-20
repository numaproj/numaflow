package cat

import (
	"context"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
)

func New() functionsdk.MapFunc {
	return func(ctx context.Context, key string, datum functionsdk.Datum) functionsdk.Messages {
		return functionsdk.MessagesBuilder().Append(functionsdk.MessageToAll(datum.Value()))
	}
}

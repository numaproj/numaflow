package cat

import (
	"context"

	"github.com/numaproj/numaflow-go/pkg/datum"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
)

func New() functionsdk.MapFunc {
	return func(ctx context.Context, key string, datum datum.Datum) functionsdk.Messages {
		return functionsdk.MessagesBuilder().Append(functionsdk.MessageToAll(datum.Value()))
	}
}

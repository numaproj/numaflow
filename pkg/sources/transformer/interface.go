package transformer

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Transformer interface {
	// Transform applies source data transformations on a batch of isb.ReadMessage.
	// it's used by source vertex mainly to 1. assign to message a user defined event time, which is used for watermark propagation.
	// and 2. to update message key which enables early data filtering.
	// As a batch operation, Transform doesn't support returning partial data.
	// It either successfully transforms all input messages and returns full result, or return an empty result if there is any error.
	Transform(context.Context, []*isb.ReadMessage) []*isb.ReadMessage
}

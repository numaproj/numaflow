package filter

import (
	"context"
	"fmt"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/shared/expr"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type filter struct {
	expression string
}

func New(args map[string]string) (functionsdk.MapFunc, error) {
	expr, existing := args["expression"]
	if !existing {
		return nil, fmt.Errorf("missing \"expression\"")
	}
	f := filter{
		expression: expr,
	}

	return func(ctx context.Context, key string, datum functionsdk.Datum) functionsdk.Messages {
		log := logging.FromContext(ctx)
		resultMsg, err := f.apply(datum.Value())
		if err != nil {
			log.Errorf("Filter map function apply got an error: %v", err)
		}
		return functionsdk.MessagesBuilder().Append(resultMsg)
	}, nil
}

func (f filter) apply(msg []byte) (functionsdk.Message, error) {

	result, err := expr.EvalBool(f.expression, msg)
	if err != nil {
		return functionsdk.MessageToDrop(), err
	}
	if result {
		return functionsdk.MessageToAll(msg), nil
	}
	return functionsdk.MessageToDrop(), nil
}

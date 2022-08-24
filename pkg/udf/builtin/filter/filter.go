package filter

import (
	"context"
	"fmt"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/shared/expr"
)

type filter struct {
	expression string
}

func New(args map[string]string) (functionsdk.DoFunc, error) {
	expr, existing := args["expression"]
	if !existing {
		return nil, fmt.Errorf("missing \"expression\"")
	}
	f := filter{
		expression: expr,
	}

	return func(ctx context.Context, key string, msg []byte) (functionsdk.Messages, error) {
		resultMsg, err := f.apply(msg)

		return functionsdk.MessagesBuilder().Append(resultMsg), err
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

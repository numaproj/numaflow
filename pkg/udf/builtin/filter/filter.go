package filter

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/shared/expr"
	funcsdk "github.com/numaproj/numaflow/sdks/golang/function"
)

type filter struct {
	expression string
}

func New(args map[string]string) (funcsdk.Handle, error) {
	expr, existing := args["expression"]
	if !existing {
		return nil, fmt.Errorf("missing \"expression\"")
	}
	f := filter{
		expression: expr,
	}
	return func(ctx context.Context, key, msg []byte) (funcsdk.Messages, error) {
		resultMsg, err := f.apply(msg)
		return funcsdk.MessagesBuilder().Append(resultMsg), err
	}, nil
}

func (f filter) apply(msg []byte) (funcsdk.Message, error) {

	result, err := expr.EvalBool(f.expression, msg)
	if err != nil {
		return funcsdk.MessageToDrop(), err
	}
	if result {
		return funcsdk.MessageToAll(msg), nil
	}
	return funcsdk.MessageToDrop(), nil
}

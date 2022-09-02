package builtin

import (
	"context"
	"fmt"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/server"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/builtin/cat"
	"github.com/numaproj/numaflow/pkg/udf/builtin/filter"
	"go.uber.org/zap"
)

type Builtin struct {
	Name   string
	Args   []string
	KWArgs map[string]string
}

func (b *Builtin) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infow("Start a builtin function", zap.Any("name", b.Name), zap.Strings("args", b.Args), zap.Any("kwargs", b.KWArgs))

	executor, err := b.executor()
	if err != nil {
		return err
	}
	server.New().RegisterMapper(executor).Start(ctx)
	return nil
}

func (b *Builtin) executor() (functionsdk.MapFunc, error) {
	// TODO: deal with args later
	switch b.Name {
	case "cat":
		return cat.New(), nil
	case "filter":
		return filter.New(b.KWArgs)
	default:
		return nil, fmt.Errorf("unrecognized function %q", b.Name)
	}
}

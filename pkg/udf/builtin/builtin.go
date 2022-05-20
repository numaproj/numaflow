package builtin

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/builtin/cat"
	"github.com/numaproj/numaflow/pkg/udf/builtin/filter"
	funcsdk "github.com/numaproj/numaflow/sdks/golang/function"
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
	excutor, err := b.excutor()
	if err != nil {
		return err
	}
	funcsdk.Start(ctx, excutor)
	return nil
}

func (b *Builtin) excutor() (funcsdk.Handle, error) {
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

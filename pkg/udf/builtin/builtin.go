/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

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

	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	eventtime "github.com/numaproj/numaflow/pkg/sources/transformer/builtin/event_time"
	"github.com/numaproj/numaflow/pkg/sources/transformer/builtin/filter"
	timeextractionfilter "github.com/numaproj/numaflow/pkg/sources/transformer/builtin/time_extraction_filter"
)

type Builtin struct {
	Name   string
	Args   []string
	KWArgs map[string]string
}

func (b *Builtin) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infow("Start a builtin transformer", zap.Any("name", b.Name), zap.Strings("args", b.Args), zap.Any("kwargs", b.KWArgs))

	executor, err := b.executor()
	if err != nil {
		return err
	}
	sourcetransformer.NewServer(executor, sourcetransformer.WithMaxMessageSize(1024*1024*64)).Start(ctx)
	return nil
}

func (b *Builtin) executor() (sourcetransformer.SourceTransformFunc, error) {
	// TODO: deal with args later
	switch b.Name {
	case "filter":
		return filter.New(b.KWArgs)
	case "eventTimeExtractor":
		return eventtime.New(b.KWArgs)
	case "timeExtractionFilter":
		return timeextractionfilter.New(b.KWArgs)
	default:
		return nil, fmt.Errorf("unrecognized transformer %q", b.Name)
	}
}

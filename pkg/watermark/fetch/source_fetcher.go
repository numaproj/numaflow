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

package fetch

import (
	"context"
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"go.uber.org/zap"
)

// sourceFetcher is a fetcher on source buffers.
type sourceFetcher struct {
	ctx              context.Context
	sourceBufferName string
	processorManager *ProcessorManager
	log              *zap.SugaredLogger
}

// NewSourceFetcher returns a new source fetcher, processorManager has the details about the processors responsible for writing to the
// buckets of the source buffer.
func NewSourceFetcher(ctx context.Context, sourceBufferName string, processorManager *ProcessorManager) Fetcher {
	return &sourceFetcher{
		ctx:              ctx,
		sourceBufferName: sourceBufferName,
		processorManager: processorManager,
		log:              logging.FromContext(ctx).With("sourceBufferName", sourceBufferName),
	}
}

// GetHeadWatermark returns the latest watermark of all the processors.
func (e *sourceFetcher) GetHeadWatermark() processor.Watermark {
	var epoch int64 = math.MinInt64
	for _, p := range e.processorManager.GetAllProcessors() {
		if !p.IsActive() {
			continue
		}
		if p.offsetTimeline.GetHeadWatermark() > epoch {
			epoch = p.offsetTimeline.GetHeadWatermark()
		}
	}
	if epoch == math.MinInt64 {
		// Use -1 as default watermark value to indicate there is no valid watermark yet.
		return processor.Watermark(time.UnixMilli(-1))
	}
	return processor.Watermark(time.UnixMilli(epoch))
}

// GetWatermark returns the lowest of the latest watermark of all the processors,
// it ignores the input offset.
func (e *sourceFetcher) GetWatermark(_ isb.Offset) processor.Watermark {
	var epoch int64 = math.MaxInt64
	for _, p := range e.processorManager.GetAllProcessors() {
		if !p.IsActive() {
			continue
		}
		if p.offsetTimeline.GetHeadWatermark() < epoch {
			epoch = p.offsetTimeline.GetHeadWatermark()
		}
	}
	if epoch == math.MaxInt64 {
		epoch = -1
	}
	return processor.Watermark(time.UnixMilli(epoch))
}

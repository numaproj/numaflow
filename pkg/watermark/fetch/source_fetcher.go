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
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// sourceFetcher is a fetcher on source buffers.
type sourceFetcher struct {
	processorManager *processorManager
	log              *zap.SugaredLogger
}

// NewSourceFetcher returns a new source fetcher, pm has the details about the processors responsible for writing to the
// buckets of the source buffer.
func NewSourceFetcher(ctx context.Context, store store.WatermarkStore, opts ...Option) SourceFetcher {
	log := logging.FromContext(ctx)
	log.Info("Creating a new source watermark fetcher")
	manager := newProcessorManager(ctx, store, 1, opts...)
	return &sourceFetcher{
		processorManager: manager,
		log:              log,
	}
}

func (e *sourceFetcher) ComputeWatermark() wmb.Watermark {
	return e.getWatermark()
}

// getWatermark returns the lowest of the latest Watermark of all the processors,
// it ignores the input Offset.
func (e *sourceFetcher) getWatermark() wmb.Watermark {
	var epoch int64 = math.MaxInt64
	var debugString strings.Builder

	for _, p := range e.processorManager.getAllProcessors() {

		if len(p.GetOffsetTimelines()) != 1 {
			e.log.Fatalf("sourceFetcher %+v has %d offset timelines, expected 1", e, len(p.GetOffsetTimelines()))
		}
		offsetTimeline := p.GetOffsetTimelines()[0]
		debugString.WriteString(fmt.Sprintf("[Processor: %v] \n", p))
		if !p.IsActive() {
			continue
		}
		if offsetTimeline.GetHeadWatermark() < epoch {
			epoch = offsetTimeline.GetHeadWatermark()
		}
	}
	if epoch == math.MaxInt64 {
		epoch = wmb.InitialWatermark.UnixMilli()
	}
	e.log.Debugf("%s get watermark for offset : %+v", debugString.String(), epoch)
	return wmb.Watermark(time.UnixMilli(epoch))
}

// ComputeHeadWatermark returns the latest watermark of all the processors for the given partition.
func (e *sourceFetcher) ComputeHeadWatermark(fromPartitionIdx int32) wmb.Watermark {
	var epoch int64 = math.MinInt64
	for _, p := range e.processorManager.getAllProcessors() {
		if !p.IsActive() {
			continue
		}
		tl := p.GetOffsetTimelines()[fromPartitionIdx]
		if tl.GetHeadWatermark() > epoch {
			epoch = tl.GetHeadWatermark()
		}
	}
	if epoch == math.MinInt64 {
		// Use -1 as default watermark value to indicate there is no valid watermark yet.
		return wmb.InitialWatermark
	}
	return wmb.Watermark(time.UnixMilli(epoch))
}

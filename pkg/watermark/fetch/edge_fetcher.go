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

// package fetch contains the logic to fetch the watermark for an offset.
// we iterate over all the active processors and get the smallest watermark.
// if the processor is not active, and if the current offset is greater than the last offset of the processor,
// we delete the processor using processor manager.

package fetch

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// edgeFetcher is a fetcher between two vertices.
type edgeFetcher struct {
	processorManager *ProcessorManager
	lastProcessedWm  []int64
	log              *zap.SugaredLogger
	sync.RWMutex
}

// NewEdgeFetcher returns a new edge fetcher.
func NewEdgeFetcher(ctx context.Context, wmStore store.WatermarkStore, fromBufferPartitionCount int, opts ...Option) *edgeFetcher {
	log := logging.FromContext(ctx)
	log.Info("Creating a new edge watermark fetcher")
	var lastProcessedWm []int64

	// to track the last processed watermark for each partition.
	for i := 0; i < fromBufferPartitionCount; i++ {
		lastProcessedWm = append(lastProcessedWm, -1)
	}

	// create the processor manager to track the processors and their offset timelines.
	manager := NewProcessorManager(ctx, wmStore, int32(fromBufferPartitionCount), opts...)

	return &edgeFetcher{
		processorManager: manager,
		lastProcessedWm:  lastProcessedWm,
		log:              log,
	}
}

// updateWatermark updates the lastProcessedWm of the given partition based on the given offset,
// deletes any inactive processor, and returns the updated watermark.
func (e *edgeFetcher) updateWatermark(inputOffset isb.Offset, fromPartitionIdx int32) wmb.Watermark {
	var offset, err = inputOffset.Sequence()
	if err != nil {
		e.log.Errorw("Unable to get offset from isb.Offset.Sequence()", zap.Error(err))
		return wmb.InitialWatermark
	}
	var debugString strings.Builder
	var epoch int64 = math.MaxInt64
	var allProcessors = e.processorManager.GetAllProcessors()
	for _, p := range allProcessors {
		// headOffset is used to check whether this pod can be deleted.
		headOffset := int64(-1)
		// iterate over all the timelines of the processor and get the smallest watermark
		debugString.WriteString(fmt.Sprintf("[Processor: %v] \n", p))
		for index, tl := range p.GetOffsetTimelines() {
			// we only need to check the timelines of the partition we are reading from
			if index == int(fromPartitionIdx) {
				var t = tl.GetEventTime(inputOffset)
				if t == -1 { // watermark cannot be computed, perhaps a new processing unit was added or offset fell off the timeline
					epoch = t
				} else if t < epoch {
					epoch = t
				}
			}
			// get the highest head offset among all the partitions of the processor so we can check later on whether the processor
			// in question is stale (its head is far behind)
			if tl.GetHeadOffset() > headOffset {
				headOffset = tl.GetHeadOffset()
			}
		}

		// if the pod is not active and the head offset of all the timelines is less than the input offset, delete the processor
		// (this means we are processing data later than what the stale processor has processed)
		if p.IsDeleted() && (offset > headOffset) {
			e.log.Infow("Deleting processor because it's stale", zap.String("processor", p.GetEntity().GetName()))
			e.processorManager.DeleteProcessor(p.GetEntity().GetName())
		}
	}
	// if there are no processors
	if epoch == math.MaxInt64 {
		epoch = -1
	}
	// update the last processed watermark for the partition
	e.Lock()
	e.lastProcessedWm[fromPartitionIdx] = epoch
	e.Unlock()

	e.log.Debugf("%s processed watermark for offset %d: %+v", debugString.String(), offset, epoch)
	return wmb.Watermark(time.UnixMilli(epoch))
}

// ComputeHeadWatermark returns the smallest head watermark among all the processors for given partition
// This can be used in showing the watermark
// progression for a vertex when not consuming the messages directly (eg. UX, tests)
// NOTE
//   - We don't use this function in the regular pods in the vertex.
//   - UX only uses ComputeHeadWatermark, so the `p.IsDeleted()` check in updateWatermark() never happens.
//     Meaning, in the UX (daemon service) we never delete any processor.
func (e *edgeFetcher) ComputeHeadWatermark(fromPartitionIdx int32) wmb.Watermark {
	var debugString strings.Builder
	var headWatermark int64 = math.MaxInt64
	var allProcessors = e.processorManager.GetAllProcessors()
	// get the head offset of each processor
	for _, p := range allProcessors {
		if !p.IsActive() {
			continue
		}
		var w = p.GetOffsetTimelines()[fromPartitionIdx].GetHeadWMB()
		e.log.Debugf("Processor: %v (headOffset:%d) (headWatermark:%d) (headIdle:%t)", p, w.Offset, w.Watermark, w.Idle)
		debugString.WriteString(fmt.Sprintf("[Processor:%v] (headOffset:%d) (headWatermark:%d) (headIdle:%t) \n", p, w.Offset, w.Watermark, w.Idle))
		if w.Offset != -1 {
			// find the smallest watermark
			if w.Watermark < headWatermark {
				headWatermark = w.Watermark
			}
		}
	}
	e.log.Debugf("ComputeHeadWatermark: %s", debugString.String())
	if headWatermark == math.MaxInt64 {
		// Use -1 as default watermark value to indicate there is no valid watermark yet.
		return wmb.InitialWatermark
	}
	return wmb.Watermark(time.UnixMilli(headWatermark))
}

// updateHeadIdleWMB updates the smallest head idle WMB for the given partition and returns the updated head idle WMB if exists.
func (e *edgeFetcher) updateHeadIdleWMB(fromPartitionIdx int32) wmb.WMB {
	var debugString strings.Builder

	var headWMB = wmb.WMB{
		// we find the head WMB based on watermark
		Offset:    math.MaxInt64,
		Watermark: math.MaxInt64,
	}
	// if any head wmb from Vn-1 processors is not idle, we skip publishing
	var allProcessors = e.processorManager.GetAllProcessors()
	for _, p := range allProcessors {
		debugString.WriteString(fmt.Sprintf("[Processor: %v] \n", p))
		if !p.IsActive() {
			continue
		}
		// find the smallest head offset among the head WMBs
		// we only consider the latest wmb in the offset timeline for the given partition
		var curHeadWMB = p.GetOffsetTimelines()[fromPartitionIdx].GetHeadWMB()
		if !curHeadWMB.Idle {
			e.log.Debugf("updateHeadIdleWMB finds an active head wmb for offset, return early")
			return wmb.WMB{}
		}
		if curHeadWMB.Watermark != -1 {
			// find the smallest head offset of the smallest WMB.watermark (though latest)
			if curHeadWMB.Watermark < headWMB.Watermark {
				headWMB = curHeadWMB
			} else if curHeadWMB.Watermark == headWMB.Watermark && curHeadWMB.Offset < headWMB.Offset {
				headWMB = curHeadWMB
			}
		}
	}
	if headWMB.Watermark == math.MaxInt64 {
		// there is no valid watermark yet
		return wmb.WMB{}
	}

	// update the last processed watermark for the partition
	e.Lock()
	e.lastProcessedWm[fromPartitionIdx] = headWMB.Watermark
	e.Unlock()

	e.log.Debugf("updateHeadIdleWMB: %s get idle head wmb for offset", debugString.String())
	return headWMB
}

// getWatermark returns the smallest last processed watermark among all the partitions.
func (e *edgeFetcher) getWatermark() wmb.Watermark {
	minWm := int64(math.MaxInt64)
	e.RLock()
	for _, wm := range e.lastProcessedWm {
		if minWm > wm {
			minWm = wm
		}
	}
	e.RUnlock()
	return wmb.Watermark(time.UnixMilli(minWm))
}

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

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// edgeFetcherSet is a set of edgeFetchers, incoming to a Vertex
// (In the case of a Join Vertex, there are multiple incoming Edges)
type edgeFetcherSet struct {
	edgeFetchers map[string]*edgeFetcher // key = name of From Vertex
	log          *zap.SugaredLogger
}

// NewEdgeFetcherSet creates a new edgeFetcherSet object which implements the Fetcher interface.
func NewEdgeFetcherSet(ctx context.Context, vertexInstance *dfv1.VertexInstance, wmStores map[string]store.WatermarkStore, opts ...Option) Fetcher {
	var edgeFetchers = make(map[string]*edgeFetcher)
	for key, wmStore := range wmStores {
		var fetchWatermark *edgeFetcher
		// create a fetcher that fetches watermark.
		if vertexInstance.Vertex.IsASource() {
			// panic: source vertex is handled using new source fetcher
			panic("NewEdgeFetcherSet can't create a new edge fetcher set for a source vertex.")
		} else if vertexInstance.Vertex.IsReduceUDF() {
			fetchWatermark = NewEdgeFetcher(ctx, wmStore, 1, opts...)
		} else {
			fetchWatermark = NewEdgeFetcher(ctx, wmStore, vertexInstance.Vertex.Spec.GetPartitionCount(), opts...)
		}
		edgeFetchers[key] = fetchWatermark
	}
	return &edgeFetcherSet{
		edgeFetchers,
		logging.FromContext(ctx),
	}
}

func (efs *edgeFetcherSet) ComputeHeadWatermark(fromPartitionIdx int32) wmb.Watermark {
	// won't be used
	return wmb.Watermark{}
}

// ComputeWatermark processes the offset on the partition indicated and returns the overall Watermark
// from all Partitions
func (efs *edgeFetcherSet) ComputeWatermark(inputOffset isb.Offset, fromPartitionIdx int32) wmb.Watermark {
	var (
		wm               wmb.Watermark
		overallWatermark = wmb.Watermark(time.UnixMilli(math.MaxInt64))
	)
	for fromVertex, fetcher := range efs.edgeFetchers {
		// we don't need to use the returned updated watermark here
		// because we do getWatermark afterwards to get
		// the overall watermark from all partitions
		_ = fetcher.updateWatermark(inputOffset, fromPartitionIdx)
		wm = fetcher.getWatermark()
		efs.log.Debugf("Got Edge watermark from vertex=%q: %v", fromVertex, wm.UnixMilli())
		if wm.BeforeWatermark(overallWatermark) {
			overallWatermark = wm
		}
	}
	return overallWatermark
}

// ComputeHeadIdleWMB returns the latest idle WMB with the smallest watermark for the given partition
// Only returns one if all Publishers are idle and if it's the smallest one of any partitions
func (efs *edgeFetcherSet) ComputeHeadIdleWMB(fromPartitionIdx int32) wmb.WMB {
	var (
		headIdleWMB, unsetWMB wmb.WMB
		overallHeadWMB        = wmb.WMB{
			// we find the head WMB based on watermark
			Offset:    math.MaxInt64,
			Watermark: math.MaxInt64,
		}
		overallWatermark = wmb.Watermark(time.UnixMilli(math.MaxInt64))
	)

	for fromVertex, fetcher := range efs.edgeFetchers {
		headIdleWMB = fetcher.updateHeadIdleWMB(fromPartitionIdx)
		if headIdleWMB == unsetWMB { // unset
			return wmb.WMB{}
		}
		efs.log.Debugf("Got Edge Head WMB from vertex=%q while processing partition %d: %v", fromVertex, fromPartitionIdx, headIdleWMB)
		if headIdleWMB.Watermark != -1 {
			// find the smallest head offset of the smallest WMB.watermark (though latest)
			if headIdleWMB.Watermark < overallHeadWMB.Watermark {
				overallHeadWMB = headIdleWMB
			} else if headIdleWMB.Watermark == overallHeadWMB.Watermark && headIdleWMB.Offset < overallHeadWMB.Offset {
				overallHeadWMB = headIdleWMB
			}
		}

		wm := fetcher.getWatermark()
		if wm.BeforeWatermark(overallWatermark) {
			overallWatermark = wm
		}
	}

	// we only consider idle watermark if it is smaller than or equal to min of all the last processed watermarks.
	if overallHeadWMB.Watermark > overallWatermark.UnixMilli() {
		return wmb.WMB{}
	}
	return overallHeadWMB

}

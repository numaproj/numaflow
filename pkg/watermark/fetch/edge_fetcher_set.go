/*
Copyright 202 The Numaproj Authors.

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
	"errors"
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"go.uber.org/zap"
)

// a set of EdgeFetchers, incoming to a Vertex
// (In the case of a Join Vertex, there are multiple incoming Edges)
type edgeFetcherSet struct {
	edgeFetchers map[string]Fetcher // key = name of From Vertex
	log          *zap.SugaredLogger
}

func NewEdgeFetcherSet(ctx context.Context, edgeFetchers map[string]Fetcher) Fetcher {
	return &edgeFetcherSet{
		edgeFetchers,
		logging.FromContext(ctx),
	}
}

// ComputeWatermark processes the offset on the partition indicated and returns the overall Watermark
// from all Partitions
func (efs *edgeFetcherSet) ComputeWatermark(inputOffset isb.Offset, fromPartitionIdx int32) wmb.Watermark {
	var wm wmb.Watermark
	overallWatermark := wmb.Watermark(time.UnixMilli(math.MaxInt64))
	for fromVertex, fetcher := range efs.edgeFetchers {
		wm = fetcher.ComputeWatermark(inputOffset, fromPartitionIdx)
		efs.log.Debugf("Got Edge watermark from vertex=%q: %v", fromVertex, wm.UnixMilli())
		if wm.BeforeWatermark(overallWatermark) {
			overallWatermark = wm
		}
	}
	return overallWatermark
}

// GetHeadWatermark returns the latest watermark among all processors for the given partition.
// This can be used in showing the watermark
// progression for a vertex when not consuming the messages directly (eg. UX, tests)
func (efs *edgeFetcherSet) GetHeadWatermark(fromPartitionIdx int32) wmb.Watermark {
	// get the most conservative time (minimum watermark) across all Edges
	var wm wmb.Watermark
	overallWatermark := wmb.Watermark(time.UnixMilli(math.MaxInt64))
	for fromVertex, fetcher := range efs.edgeFetchers {
		wm = fetcher.GetHeadWatermark(fromPartitionIdx)
		if wm == wmb.InitialWatermark { // unset
			continue
		}
		efs.log.Debugf("Got Edge Head Watermark from vertex=%q while processing partition %d: %v", fromVertex, fromPartitionIdx, wm.UnixMilli())
		if wm.BeforeWatermark(overallWatermark) {
			overallWatermark = wm
		}
	}
	return overallWatermark
}

// GetHeadWMB returns the latest idle WMB with the smallest watermark for the given partition
// Only returns one if all Publishers are idle and if it's the smallest one of any partitions
func (efs *edgeFetcherSet) GetHeadWMB(fromPartitionIdx int32) wmb.WMB {
	// if we get back one that's empty it means that there could be one that's not Idle, so we need to return empty

	// call GetHeadWMB() for all Edges and get the smallest one
	var watermarkBuffer, unsetWMB wmb.WMB
	var overallHeadWMB = wmb.WMB{
		// we find the head WMB based on watermark
		Offset:    math.MaxInt64,
		Watermark: math.MaxInt64,
	}

	for fromVertex, fetcher := range efs.edgeFetchers {
		watermarkBuffer = fetcher.GetHeadWMB(fromPartitionIdx)
		if watermarkBuffer == unsetWMB { // unset
			return wmb.WMB{}
		}
		efs.log.Debugf("Got Edge Head WMB from vertex=%q while processing partition %d: %v", fromVertex, fromPartitionIdx, watermarkBuffer)
		if watermarkBuffer.Watermark != -1 {
			// find the smallest head offset of the smallest WMB.watermark (though latest)
			if watermarkBuffer.Watermark < overallHeadWMB.Watermark {
				overallHeadWMB = watermarkBuffer
			} else if watermarkBuffer.Watermark == overallHeadWMB.Watermark && watermarkBuffer.Offset < overallHeadWMB.Offset {
				overallHeadWMB = watermarkBuffer
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// TODO(join): the check below has been temporarily moved from here to EdgeFetcher::GetHeadWMB() so that EdgeFetcher::GetHeadWMB()
	// can maintain its existing contract.
	// Note that this means that method will return wmb.WMB{} some of the time which will cause this to do the same
	// even in cases where the overall Idle WMB < overall last processed watermark.
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// we only consider idle watermark if it is smaller than or equal to min of all the last processed watermarks.
	//if overallHeadWMB.Watermark > efs.GetWatermark().UnixMilli() {
	//	return wmb.WMB{}
	//}
	return overallHeadWMB

}

func (efs *edgeFetcherSet) Close() error {
	aggregateErr := ""
	for _, fetcher := range efs.edgeFetchers {
		err := fetcher.Close()
		if err != nil {
			aggregateErr += err.Error() + "; "
		}
	}
	if aggregateErr != "" {
		return errors.New(aggregateErr)
	}
	return nil
}

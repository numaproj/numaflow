/*
Copyright 2023 The Numaproj Authors.

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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// a set of EdgeFetchers, incoming to a Vertex
// (In the case of a Join Vertex, there are multiple incoming Edges)
// key=name of From Vertex
type EdgeFetcherSet map[string]Fetcher

// GetWatermark processes the Watermark for the given partition from the given offset and return the Watermark across
// all partitions
func (efs *EdgeFetcherSet) GetWatermark(inputOffset isb.Offset, fromPartitionIdx int32) wmb.Watermark {

}

// GetHeadWatermark returns the latest watermark among all processors for the given partition.
// This can be used in showing the watermark
// progression for a vertex when not consuming the messages directly (eg. UX, tests)
func (efs *EdgeFetcherSet) GetHeadWatermark(fromPartitionIdx int32) wmb.Watermark {

}

// GetHeadWMB returns the latest idle WMB with the smallest watermark for the given partition
// Only returns one if all Publishers are idle and if it's the smallest one of any partitions
func (efs *EdgeFetcherSet) GetHeadWMB(fromPartitionIdx int32) wmb.WMB {
	// if we get back one that's empty it means that there could be one that's not Idle, so we need to return empty

	// todo: need to change the EdgeFetcher method to return a value even if it's not the smallest partition,
	// and we can instead do the check for smallest here by calling EdgeFetcherSet.getMinFromLastProcessed

}

func (efs *EdgeFetcherSet) Close() error {

}

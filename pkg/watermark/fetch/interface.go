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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// Fetcher fetches watermark data from Vn-1 vertex and computes the watermark for Vn.
type Fetcher interface {
	// ComputeWatermark computes a valid watermark for the given offset on the given partition
	ComputeWatermark(offset isb.Offset, fromPartitionIdx int32) wmb.Watermark
	// ComputeHeadIdleWMB computes a valid head idle WMB for the given partition.
	ComputeHeadIdleWMB(fromPartitionIdx int32) wmb.WMB
}

// SourceFetcher fetches watermark data for source vertex.
type SourceFetcher interface {
	// ComputeWatermark computes the watermark, it will return the minimum of all the watermarks of the processors.
	ComputeWatermark() wmb.Watermark
	HeadFetcher
}

// HeadFetcher computes the watermark for Vn.
type HeadFetcher interface {
	// ComputeHeadWatermark computes a valid head watermark for the given partition
	ComputeHeadWatermark(fromPartitionIdx int32) wmb.Watermark
}

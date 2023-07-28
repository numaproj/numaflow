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
	"io"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// Fetcher fetches watermark data from Vn-1 vertex.
type Fetcher interface {
	io.Closer
	// ComputeWatermark processes the offset on the partition indicated and returns the overall Watermark
	// from all Partitions
	ComputeWatermark(offset isb.Offset, fromPartitionIdx int32) wmb.Watermark
	// GetHeadWatermark returns the latest watermark among all processors
	GetHeadWatermark(fromPartitionIdx int32) wmb.Watermark
	// GetHeadWMB returns the latest idle WMB among all processors
	GetHeadWMB(fromPartitionIdx int32) wmb.WMB
}

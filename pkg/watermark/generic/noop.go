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

package generic

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// NoOpWMProgressor is a no-op watermark progressor. As the name suggests, it does not do anything, no watermark is
// progressed. NoOpWMProgressor is used when watermarking is disabled.
type NoOpWMProgressor struct {
}

var _ fetch.Fetcher = (*NoOpWMProgressor)(nil)
var _ publish.Publisher = (*NoOpWMProgressor)(nil)

// NewNoOpWMProgressor returns NoOpWMProgressor.
func NewNoOpWMProgressor() *NoOpWMProgressor {
	return &NoOpWMProgressor{}
}

func (n NoOpWMProgressor) ComputeWatermark(isb.Offset, int32) wmb.Watermark {
	return wmb.Watermark{}
}

// PublishWatermark does a no-op watermark publish.
func (n NoOpWMProgressor) PublishWatermark(wmb.Watermark, isb.Offset, int32) {
}

// PublishIdleWatermark does a no-op idle watermark publish.
func (n NoOpWMProgressor) PublishIdleWatermark(wmb.Watermark, isb.Offset, int32) {
}

// GetLatestWatermark returns the default watermark as the latest watermark.
func (n NoOpWMProgressor) GetLatestWatermark() wmb.Watermark {
	return wmb.Watermark{}
}

// ComputeHeadWatermark returns the default head watermark.
func (n NoOpWMProgressor) ComputeHeadWatermark(int32) wmb.Watermark {
	return wmb.Watermark{}
}

// ComputeHeadIdleWMB returns the default WMB.
func (n NoOpWMProgressor) ComputeHeadIdleWMB(int32) wmb.WMB {
	return wmb.WMB{}
}

// Close stops the no-op progressor.
func (n NoOpWMProgressor) Close() error {
	return nil
}

func BuildNoOpWatermarkProgressorsFromBufferList(toBuffers []string) (fetch.Fetcher, map[string]publish.Publisher) {
	fetchWatermark := NewNoOpWMProgressor()
	publishWatermark := make(map[string]publish.Publisher)
	for _, buffer := range toBuffers {
		publishWatermark[buffer] = NewNoOpWMProgressor()
	}
	return fetchWatermark, publishWatermark
}

func BuildNoOpWatermarkProgressorsFromBufferMap(bufferMap map[string][]isb.BufferWriter) (fetch.Fetcher, map[string]publish.Publisher) {
	fetchWatermark := NewNoOpWMProgressor()
	publishWatermark := make(map[string]publish.Publisher)
	for buffName := range bufferMap {
		publishWatermark[buffName] = NewNoOpWMProgressor()
	}
	return fetchWatermark, publishWatermark
}

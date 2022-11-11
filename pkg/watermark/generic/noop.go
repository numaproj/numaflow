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
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
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

// GetWatermark returns the default watermark.
func (n NoOpWMProgressor) GetWatermark(_ isb.Offset) processor.Watermark {
	return processor.Watermark{}
}

// PublishWatermark does a no-op publish.
func (n NoOpWMProgressor) PublishWatermark(_ processor.Watermark, _ isb.Offset) {
}

// GetLatestWatermark returns the default watermark as the latest watermark.
func (n NoOpWMProgressor) GetLatestWatermark() processor.Watermark {
	return processor.Watermark{}
}

// GetHeadWatermark returns the default head watermark.
func (n NoOpWMProgressor) GetHeadWatermark() processor.Watermark {
	return processor.Watermark{}
}

// StopPublisher stops the no-op publisher.
func (n NoOpWMProgressor) StopPublisher() {
}

func BuildNoOpWatermarkProgressorsFromEdgeList(bufferList []string) (fetch.Fetcher, map[string]publish.Publisher) {
	fetchWatermark := NewNoOpWMProgressor()
	publishWatermark := make(map[string]publish.Publisher)
	for _, buffer := range bufferList {
		publishWatermark[buffer] = NewNoOpWMProgressor()
	}
	return fetchWatermark, publishWatermark
}

func BuildNoOpWatermarkProgressorsFromBufferMap(bufferMap map[string]isb.BufferWriter) (fetch.Fetcher, map[string]publish.Publisher) {
	fetchWatermark := NewNoOpWMProgressor()
	publishWatermark := make(map[string]publish.Publisher)
	for buffName := range bufferMap {
		publishWatermark[buffName] = NewNoOpWMProgressor()
	}
	return fetchWatermark, publishWatermark
}

func GetBufferNameList(bufferList []v1alpha1.Buffer) []string {
	bufferName := make([]string, len(bufferList))
	for idx, buffer := range bufferList {
		bufferName[idx] = buffer.Name
	}
	return bufferName
}

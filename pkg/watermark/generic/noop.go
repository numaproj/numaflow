package generic

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// NoOpWMProgressor is a no-op watermark progressor. As the name suggests, it does not do anything, no watermark is
// progressed.
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

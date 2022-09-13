package fetch

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
)

// Fetcher fetches watermark data from Vn-1 vertex.
type Fetcher interface {
	// GetWatermark returns the inorder monotonically increasing watermark of the edge connected to Vn-1.
	GetWatermark(offset isb.Offset) processor.Watermark
	// GetHeadWatermark returns the latest watermark based on the head offset
	GetHeadWatermark() processor.Watermark
}

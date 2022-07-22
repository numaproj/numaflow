package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// GenericFetch is a generic fetcher which can be used for most use cases.
type GenericFetch struct {
	fromEdge *fetch.Edge
}

var _ fetch.Fetcher = (*GenericFetch)(nil)

// FetchWMWatchers has the watcher information required for fetching watermarks.
type FetchWMWatchers struct {
	HBWatch store.WatermarkKVWatcher
	OTWatch store.WatermarkKVWatcher
}

// BuildFetchWMWatchers builds the FetchWMWatchers
func BuildFetchWMWatchers(hbWatch store.WatermarkKVWatcher, otWatch store.WatermarkKVWatcher) FetchWMWatchers {
	return FetchWMWatchers{
		HBWatch: hbWatch,
		OTWatch: otWatch,
	}
}

// NewGenericFetch returns GenericFetch. vertexName is the vertex currently processing.
// fetchWM is a struct for retrieving both the heartbeat
// and the offset watermark timeline (Vn-1 vertex).
func NewGenericFetch(ctx context.Context, vertexName string, fetchWM FetchWMWatchers) *GenericFetch {
	fromVertex := fetch.NewFromVertex(ctx, fetchWM.HBWatch, fetchWM.OTWatch)
	fromEdge := fetch.NewEdgeBuffer(ctx, vertexName, fromVertex)

	gf := &GenericFetch{
		fromEdge: fromEdge,
	}

	return gf
}

// GetWatermark returns the watermark for the offset.
func (g *GenericFetch) GetWatermark(offset isb.Offset) processor.Watermark {
	return g.fromEdge.GetWatermark(offset)
}

// GetHeadWatermark returns the head watermark based on the head offset.
func (g *GenericFetch) GetHeadWatermark() processor.Watermark {
	return g.fromEdge.GetHeadWatermark()
}

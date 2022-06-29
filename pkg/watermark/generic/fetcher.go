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

type FetchWM struct {
	hbWatch store.WatermarkKVWatcher
	otWatch store.WatermarkKVWatcher
}

// BuildFetchWM builds the FetchWM
func BuildFetchWM(hbWatch store.WatermarkKVWatcher, otWatch store.WatermarkKVWatcher) FetchWM {
	return FetchWM{
		hbWatch: hbWatch,
		otWatch: otWatch,
	}
}

// NewGenericFetcher returns GenericFetch.
func NewGenericFetcher(ctx context.Context, vertexName string, fetchKeyspace string, fetchWM FetchWM) *GenericFetch {
	fromVertex := fetch.NewFromVertex(ctx, fetchKeyspace, fetchWM.hbWatch, fetchWM.otWatch)
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

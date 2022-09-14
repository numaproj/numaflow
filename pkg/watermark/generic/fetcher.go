package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

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

// NewGenericEdgeFetch returns a Fetcher, where bufferName is the from buffer of the vertex that is currently processing.
// fetchWM is a struct for retrieving both the heartbeat
// and the offset watermark timeline (Vn-1 vertex).
func NewGenericEdgeFetch(ctx context.Context, bufferName string, fetchWM FetchWMWatchers) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, fetchWM.HBWatch, fetchWM.OTWatch)
	return fetch.NewEdgeFetcher(ctx, bufferName, processorManager)
}

// NewGenericSourceFetch returns Fetcher, where sourceBufferName is the source buffer of the source vertex.
func NewGenericSourceFetch(ctx context.Context, sourceBufferName string, fetchWM FetchWMWatchers) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, fetchWM.HBWatch, fetchWM.OTWatch)
	return fetch.NewSourceFetcher(ctx, sourceBufferName, processorManager)
}

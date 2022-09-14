package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// NewGenericEdgeFetch returns a Fetcher, where bufferName is the from buffer of the vertex that is currently processing.
// fetchWM is a struct for retrieving both the heartbeat
// and the offset watermark timeline (Vn-1 vertex).
func NewGenericEdgeFetch(ctx context.Context, bufferName string, storeWatcher store.WatermarkStoreWatcher) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, storeWatcher)
	return fetch.NewEdgeFetcher(ctx, bufferName, processorManager)
}

// NewGenericSourceFetch returns Fetcher, where sourceBufferName is the source buffer of the source vertex.
func NewGenericSourceFetch(ctx context.Context, sourceBufferName string, storeWatcher store.WatermarkStoreWatcher) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, storeWatcher)
	return fetch.NewSourceFetcher(ctx, sourceBufferName, processorManager)
}

package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// NewGenericEdgeFetch returns a Fetcher, which fetches watermarks from bufferName by consulting the corresponding storeWatcher
func NewGenericEdgeFetch(ctx context.Context, bufferName string, storeWatcher store.WatermarkStoreWatcher) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, storeWatcher)
	return fetch.NewEdgeFetcher(ctx, bufferName, processorManager)
}

// NewGenericSourceFetch returns Fetcher, where sourceBufferName is the source buffer of the source vertex.
// TODO remove this - we no longer need a source fetcher since we now look at the out edge to determine watermark for a vertex.
func NewGenericSourceFetch(ctx context.Context, sourceBufferName string, storeWatcher store.WatermarkStoreWatcher) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, storeWatcher)
	return fetch.NewSourceFetcher(ctx, sourceBufferName, processorManager)
}

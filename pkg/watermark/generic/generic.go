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
func NewGenericSourceFetch(ctx context.Context, sourceBufferName string, storeWatcher store.WatermarkStoreWatcher) fetch.Fetcher {
	processorManager := fetch.NewProcessorManager(ctx, storeWatcher)
	return fetch.NewSourceFetcher(ctx, sourceBufferName, processorManager)
}

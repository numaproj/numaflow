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

// Package generic implements some shareable watermarking progressors (fetcher and publisher) and methods.

package jetstream

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/kvs/noop"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// BuildWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// The function is used only when watermarking is enabled on the pipeline.
func BuildWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (fetch.Fetcher, map[string]publish.Publisher, error) {
	// if watermark is not enabled, use no-op.
	if vertexInstance.Vertex.Spec.Watermark.Disabled {
		names := vertexInstance.Vertex.GetToBuffers()
		if vertexInstance.Vertex.IsASink() {
			// Sink has no to buffers, we use the vertex name as the buffer writer name.
			names = append(names, vertexInstance.Vertex.Spec.Name)
		}
		fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(names)
		return fetchWatermark, publishWatermark, nil
	}

	pipelineName := vertexInstance.Vertex.Spec.PipelineName

	fetchWatermark, err := BuildFetcher(ctx, vertexInstance, client)
	if err != nil {
		return nil, nil, err
	}

	publishWatermark, err := buildPublishers(ctx, pipelineName, vertexInstance, client)
	if err != nil {
		return nil, nil, err
	}

	return fetchWatermark, publishWatermark, nil
}

// BuildFetcher creates a Fetcher (implemented by EdgeFetcherSet) which is used to fetch the Watermarks for a given Vertex
// (for all incoming Edges) and resolve the overall Watermark for the Vertex
func BuildFetcher(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (fetch.Fetcher, error) {
	// if watermark is not enabled, use no-op.
	if vertexInstance.Vertex.Spec.Watermark.Disabled {
		return nil, fmt.Errorf("watermark disabled")
	}

	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	edgeFetchers := make(map[string]fetch.Fetcher)

	vertex := vertexInstance.Vertex
	if vertex.IsASource() {
		fromBucket := v1alpha1.GenerateSourceBucketName(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name)
		edgeFetcher, err := buildFetcherForBucket(ctx, vertexInstance, fromBucket, client)
		if err != nil {
			return nil, err
		}
		// For source vertex, we use the vertex name as the from buffer name
		edgeFetchers[vertex.Spec.Name] = edgeFetcher
	} else {
		for _, e := range vertex.Spec.FromEdges {
			fromBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
			edgeFetcher, err := buildFetcherForBucket(ctx, vertexInstance, fromBucket, client)
			if err != nil {
				return nil, err
			}
			edgeFetchers[e.From] = edgeFetcher
		}
	}

	return fetch.NewEdgeFetcherSet(ctx, edgeFetchers), nil
}

// buildFetcherForBucket creates a Fetcher (implemented by EdgeFetcher) which is used to fetch the Watermarks for a single incoming Edge
// to a Vertex (a single Edge has a single Bucket)
func buildFetcherForBucket(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, fromBucket string, client *jsclient.NATSClient) (fetch.Fetcher, error) {
	var fetchWatermark fetch.Fetcher
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	hbBucketName := isbsvc.JetStreamProcessorBucket(fromBucket)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucketName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVWatch, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	otBucketName := isbsvc.JetStreamOTBucket(fromBucket)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucketName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVWatch, OTBucket: %s, %w", otBucketName, err)
	}

	// create a store watcher that watches the heartbeat and ot store.
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatch, otWatch)
	// create processor manager with the store watcher which will keep track of all the active processors and updates the offset timelines accordingly.
	processManager := processor.NewProcessorManager(ctx, storeWatcher, fromBucket, int32(len(vertexInstance.Vertex.OwnedBuffers())),
		processor.WithVertexReplica(vertexInstance.Replica), processor.WithIsReduce(vertexInstance.Vertex.IsReduceUDF()), processor.WithIsSource(vertexInstance.Vertex.IsASource()))

	// create a fetcher that fetches watermark.
	if vertexInstance.Vertex.IsASource() {
		fetchWatermark = fetch.NewSourceFetcher(ctx, processManager)
	} else if vertexInstance.Vertex.IsReduceUDF() {
		fetchWatermark = fetch.NewEdgeFetcher(ctx, processManager, 1)
	} else {
		fetchWatermark = fetch.NewEdgeFetcher(ctx, processManager, vertexInstance.Vertex.Spec.GetPartitionCount())
	}

	return fetchWatermark, nil
}

// buildPublishers creates the Watermark Publishers for a given Vertex, one per Edge
func buildPublishers(ctx context.Context, pipelineName string, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]publish.Publisher, error) {
	// Publisher map creation, we need a publisher per out buffer.
	var publishWatermark = make(map[string]publish.Publisher)
	var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	publishEntity := processor.NewProcessorEntity(processorName)
	if vertexInstance.Vertex.IsASink() {
		toBucket := vertexInstance.Vertex.GetToBuckets()[0]
		hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
		}

		otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
		}
		// For sink vertex, we use the vertex name as the to buffer name, which is the key for the publisher map.
		publishWatermark[vertexInstance.Vertex.Spec.Name] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore), 1, publish.IsSink())
	} else {
		for _, e := range vertexInstance.Vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
			hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
			hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
			}

			otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
			otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
			}
			publishWatermark[e.To] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore), int32(e.GetToVertexPartitionCount()))
		}
	}

	return publishWatermark, nil
}

// BuildSourcePublisherStores builds the watermark stores for source publisher.
func BuildSourcePublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (store.WatermarkStore, error) {
	if !vertexInstance.Vertex.IsASource() {
		return nil, fmt.Errorf("not a source vertex")
	}
	if vertexInstance.Vertex.Spec.Watermark.Disabled {
		return store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore()), nil
	}
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	bucketName := vertexInstance.Vertex.GetFromBuckets()[0]
	// heartbeat
	hbBucketName := isbsvc.JetStreamProcessorBucket(bucketName)
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucketName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore for source, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	// OT
	otStoreBucketName := isbsvc.JetStreamOTBucket(bucketName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVStore for source, OTBucket: %s, %w", otStoreBucketName, err)
	}
	sourcePublishStores := store.BuildWatermarkStore(hbKVStore, otKVStore)
	return sourcePublishStores, nil
}

func BuildToVertexPublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]store.WatermarkStore, error) {
	var pipelineName = vertexInstance.Vertex.Spec.PipelineName
	var publisherStores = make(map[string]store.WatermarkStore)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		toBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
		hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
		}

		otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
		}
		publisherStores[e.To] = store.BuildWatermarkStore(hbStore, otStore)
	}
	return publisherStores, nil
}

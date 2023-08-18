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
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// BuildFetcher creates a Fetcher (implemented by EdgeFetcherSet) which is used to fetch the Watermarks for a given Vertex
// (for all incoming Edges) and resolve the overall Watermark for the Vertex
func BuildFetcher(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (fetch.Fetcher, error) {
	// if watermark is not enabled, use no-op.
	if vertexInstance.Vertex.Spec.Watermark.Disabled {
		return nil, fmt.Errorf("watermark disabled")
	}

	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	processorManagers := make(map[string]*processor.ProcessorManager)

	vertex := vertexInstance.Vertex
	if vertex.IsASource() {
		fromBucket := v1alpha1.GenerateSourceBucketName(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name)
		processorManager, err := buildProcessorManagerForBucket(ctx, vertexInstance, fromBucket, client)
		if err != nil {
			return nil, err
		}
		return fetch.NewSourceFetcher(ctx, processorManager), nil
	} else {
		for _, e := range vertex.Spec.FromEdges {
			fromBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
			processorManager, err := buildProcessorManagerForBucket(ctx, vertexInstance, fromBucket, client)
			if err != nil {
				return nil, err
			}
			processorManagers[e.From] = processorManager
		}
	}

	return fetch.NewEdgeFetcherSet(ctx, vertexInstance, processorManagers), nil
}

func BuildProcessorManagers(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]*processor.ProcessorManager, error) {
	var managers = make(map[string]*processor.ProcessorManager)
	var fromBucket string
	vertex := vertexInstance.Vertex
	if vertex.IsASource() {
		fromBucket = v1alpha1.GenerateSourceBucketName(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name)
		processManager, err := buildProcessorManagerForBucket(ctx, vertexInstance, fromBucket, client)
		if err != nil {
			return nil, err
		}
		managers[vertex.Name] = processManager
	} else {
		for _, e := range vertex.Spec.FromEdges {
			fromBucket = v1alpha1.GenerateEdgeBucketName(vertex.Namespace, vertex.Spec.PipelineName, e.From, e.To)
			processManager, err := buildProcessorManagerForBucket(ctx, vertexInstance, fromBucket, client)
			if err != nil {
				return nil, err
			}
			managers[e.From] = processManager
		}
	}
	return managers, nil
}

// buildProcessorManagerForBucket creates a processor manager for the given bucket.
func buildProcessorManagerForBucket(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, fromBucket string, client *jsclient.NATSClient) (*processor.ProcessorManager, error) {
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

	return processManager, nil
}

// BuildToVertexWatermarkStores creates a map of WatermarkStore for all the to buckets of the given vertex.
func BuildToVertexWatermarkStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]store.WatermarkStore, error) {
	var wmStores = make(map[string]store.WatermarkStore)
	vertex := vertexInstance.Vertex
	pipelineName := vertex.Spec.PipelineName

	if vertex.IsASink() {
		toBucket := vertex.GetToBuckets()[0]

		// build heartBeat store
		hbBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucketName, client)

		// build offsetTimeline store
		otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)

		if err != nil {
			return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore, HeartbeatBucket: %s, %w", hbBucketName, err)
		}
		// build watermark store using the hb and ot store
		wmStores[toBucket] = store.BuildWatermarkStore(hbStore, otStore)
	} else {
		for _, e := range vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertex.Namespace, vertex.Spec.PipelineName, e.From, e.To)

			// build heartBeat store
			hbBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
			hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, vertex.Spec.PipelineName, hbBucketName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore, HeartbeatBucket: %s, %w", hbBucketName, err)
			}

			// build offsetTimeline store
			otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
			otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
			}

			// build watermark store using the hb and ot store
			wmStores[toBucket] = store.BuildWatermarkStore(hbStore, otStore)
		}
	}

	return wmStores, nil
}

// BuildPublishersFromStores creates a map of publishers for all the to buckets of the given vertex using the given watermark stores.
func BuildPublishersFromStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, wmStores map[string]store.WatermarkStore) map[string]publish.Publisher {
	// Publisher map creation, we need a publisher per out buffer.
	var (
		publishWatermark = make(map[string]publish.Publisher)
		processorName    = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
		pipelineName     = vertexInstance.Vertex.Spec.PipelineName
		vertex           = vertexInstance.Vertex
	)

	publishEntity := processor.NewProcessorEntity(processorName)

	if vertex.IsASink() {
		toBucket := vertex.GetToBuckets()[0]
		wmStore := wmStores[toBucket]
		publishWatermark[vertex.Spec.Name] = publish.NewPublish(ctx, publishEntity, wmStore, 1, publish.IsSink())
	} else {
		for _, e := range vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertex.Namespace, pipelineName, e.From, e.To)
			wmStore := wmStores[toBucket]
			publishWatermark[toBucket] = publish.NewPublish(ctx, publishEntity, wmStore, int32(e.GetToVertexPartitionCount()))
		}
	}
	return publishWatermark
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

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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// BuildProcessorManagers creates a map of ProcessorManagers for all the incoming edges of the given Vertex.
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
	// create a store watcher that watches the heartbeat and ot store.
	storeWatcher, err := store.BuildJetStreamWatermarkStoreWatcher(ctx, fromBucket, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new JetStream watermark store watcher, %w", err)
	}

	log := logging.FromContext(ctx).With("bucket", fromBucket)
	// create processor manager with the store watcher which will keep track of all the active processors and updates the offset timelines accordingly.
	processManager := processor.NewProcessorManager(logging.WithLogger(ctx, log), storeWatcher, int32(len(vertexInstance.Vertex.OwnedBuffers())),
		processor.WithVertexReplica(vertexInstance.Replica), processor.WithIsReduce(vertexInstance.Vertex.IsReduceUDF()), processor.WithIsSource(vertexInstance.Vertex.IsASource()))

	return processManager, nil
}

// BuildToVertexWatermarkStores creates a map of WatermarkStore for all the to buckets of the given vertex.
func BuildToVertexWatermarkStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]store.WatermarkStore, error) {
	var wmStores = make(map[string]store.WatermarkStore)
	vertex := vertexInstance.Vertex

	if vertex.IsASink() {
		toBucket := vertex.GetToBuckets()[0]
		// build watermark store
		wmStore, err := store.BuildJetStreamWatermarkStore(ctx, toBucket, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new JetStream watermark store, %w", err)
		}
		wmStores[vertex.Spec.Name] = wmStore
	} else {
		for _, e := range vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertex.Namespace, vertex.Spec.PipelineName, e.From, e.To)
			// build watermark store
			wmStore, err := store.BuildJetStreamWatermarkStore(ctx, toBucket, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new JetStream watermark store, %w", err)
			}

			// build watermark store using the hb and ot store
			wmStores[e.To] = wmStore
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
		vertex           = vertexInstance.Vertex
	)

	publishEntity := processor.NewProcessorEntity(processorName)

	if vertex.IsASink() {
		wmStore := wmStores[vertex.Spec.Name]
		publishWatermark[vertex.Spec.Name] = publish.NewPublish(ctx, publishEntity, wmStore, 1, publish.IsSink())
	} else {
		for _, e := range vertex.Spec.ToEdges {
			wmStore := wmStores[e.To]
			publishWatermark[e.To] = publish.NewPublish(ctx, publishEntity, wmStore, int32(e.GetToVertexPartitionCount()))
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
		return store.BuildNoOpWatermarkStore()
	}
	bucketName := vertexInstance.Vertex.GetFromBuckets()[0]
	wmStore, err := store.BuildJetStreamWatermarkStore(ctx, bucketName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new JetStream watermark store, %w", err)
	}

	return wmStore, nil
}

func BuildToVertexPublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]store.WatermarkStore, error) {
	var pipelineName = vertexInstance.Vertex.Spec.PipelineName
	var publisherStores = make(map[string]store.WatermarkStore)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		toBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
		wmStore, err := store.BuildJetStreamWatermarkStore(ctx, toBucket, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new JetStream watermark store, %w", err)
		}

		publisherStores[e.To] = wmStore
	}
	return publisherStores, nil
}

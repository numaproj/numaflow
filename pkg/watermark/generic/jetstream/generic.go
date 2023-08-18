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
	"github.com/numaproj/numaflow/pkg/watermark/publish"
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
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	hbKVName := isbsvc.JetStreamProcessorKVName(fromBucket)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVWatch, hbKVName: %s, %w", hbKVName, err)
	}

	otKVName := isbsvc.JetStreamOTKVName(fromBucket)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVWatch, otKVName: %s, %w", otKVName, err)
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
		hbKVName := isbsvc.JetStreamProcessorKVName(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbKVName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore, HeartbeatKV: %s, %w", hbKVName, err)
		}

		// build offsetTimeline store
		otStoreKVName := isbsvc.JetStreamOTKVName(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreKVName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new OT KVJetStreamKVStore, OffsetTimelineKV: %s, %w", otStoreKVName, err)
		}
		// build watermark store using the hb and ot store
		wmStores[vertex.Spec.Name] = store.BuildWatermarkStore(hbStore, otStore)
	} else {
		for _, e := range vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertex.Namespace, vertex.Spec.PipelineName, e.From, e.To)

			// build heartBeat store
			hbKVName := isbsvc.JetStreamProcessorKVName(toBucket)
			hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, vertex.Spec.PipelineName, hbKVName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore, HeartbeatKV: %s, %w", hbKVName, err)
			}

			// build offsetTimeline store
			otStoreKVName := isbsvc.JetStreamOTKVName(toBucket)
			otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreKVName, client)
			if err != nil {
				return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, otKVName: %s, %w", otStoreKVName, err)
			}

			// build watermark store using the hb and ot store
			wmStores[e.To] = store.BuildWatermarkStore(hbStore, otStore)
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
		return store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore()), nil
	}
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	bucketName := vertexInstance.Vertex.GetFromBuckets()[0]
	// heartbeat
	hbKVName := isbsvc.JetStreamProcessorKVName(bucketName)
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore for source, hbKVName: %s, %w", hbKVName, err)
	}

	// OT
	otKVName := isbsvc.JetStreamOTKVName(bucketName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVStore for source, otKVName: %s, %w", otKVName, err)
	}
	sourcePublishStores := store.BuildWatermarkStore(hbKVStore, otKVStore)
	return sourcePublishStores, nil
}

func BuildToVertexPublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance, client *jsclient.NATSClient) (map[string]store.WatermarkStore, error) {
	var pipelineName = vertexInstance.Vertex.Spec.PipelineName
	var publisherStores = make(map[string]store.WatermarkStore)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		toBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
		hbPublisherKVName := isbsvc.JetStreamProcessorKVName(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherKVName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherKV: %s, %w", hbPublisherKVName, err)
		}

		otKVName := isbsvc.JetStreamOTKVName(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otKVName, client)
		if err != nil {
			return nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, otKVName: %s, %w", otKVName, err)
		}
		publisherStores[e.To] = store.BuildWatermarkStore(hbStore, otStore)
	}
	return publisherStores, nil
}

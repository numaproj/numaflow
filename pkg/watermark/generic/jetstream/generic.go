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

	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
)

// BuildWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// The function is used only when watermarking is enabled on the pipeline.
func BuildWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetch.Fetcher, map[string]publish.Publisher, error) {
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
	fromBucket := vertexInstance.Vertex.GetFromBuckets()[0]

	// TODO(one bucket): remove this after we combine the buckets to one for reduce.
	if vertexInstance.Vertex.IsReduceUDF() {
		fromBucket = fmt.Sprintf("%s-%d", fromBucket, vertexInstance.Replica)
	}

	var fetchWatermark fetch.Fetcher
	hbBucketName := isbsvc.JetStreamProcessorBucket(fromBucket)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new HB KVJetStreamKVWatch, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	otBucketName := isbsvc.JetStreamOTBucket(fromBucket)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new OT KVJetStreamKVWatch, OTBucket: %s, %w", otBucketName, err)
	}

	if vertexInstance.Vertex.IsASource() {
		fetchWatermark = fetch.NewSourceFetcher(ctx, fromBucket, store.BuildWatermarkStoreWatcher(hbWatch, otWatch))
	} else {
		fetchWatermark = fetch.NewEdgeFetcher(ctx, fromBucket, store.BuildWatermarkStoreWatcher(hbWatch, otWatch))
	}

	// Publisher map creation, we need a publisher per out buffer.
	var publishWatermark = make(map[string]publish.Publisher)
	var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	publishEntity := processor.NewProcessorEntity(processorName)
	if vertexInstance.Vertex.IsASink() {
		toBucket := vertexInstance.Vertex.GetToBuckets()[0]
		hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
		}

		otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
		}
		// For sink vertex, we use the vertex name as the to buffer name, which is the key for the publisher map.
		publishWatermark[vertexInstance.Vertex.Spec.Name] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore), publish.IsSink())
	} else {
		for _, e := range vertexInstance.Vertex.Spec.ToEdges {
			toBucket := v1alpha1.GenerateEdgeBucketName(vertexInstance.Vertex.Namespace, pipelineName, e.From, e.To)
			// TODO(one bucket): remove this after we combine the buckets to one for reduce.
			if e.ToVertexType == v1alpha1.VertexTypeReduceUDF {
				for i := 0; i < e.GetToVertexPartitions(); i++ {
					indexedBucket := fmt.Sprintf("%s-%d", toBucket, i)
					toBufferName := v1alpha1.GenerateBufferName(vertexInstance.Vertex.Namespace, pipelineName, e.To, i)
					hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(indexedBucket)
					hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, jsclient.NewInClusterJetStreamClient())
					if err != nil {
						return nil, nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
					}

					otStoreBucketName := isbsvc.JetStreamOTBucket(indexedBucket)
					otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
					if err != nil {
						return nil, nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
					}
					publishWatermark[toBufferName] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore))
				}
			} else {
				hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(toBucket)
				hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, jsclient.NewInClusterJetStreamClient())
				if err != nil {
					return nil, nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
				}

				otStoreBucketName := isbsvc.JetStreamOTBucket(toBucket)
				otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
				if err != nil {
					return nil, nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
				}
				opts := []publish.PublishOption{}
				if vertexInstance.Vertex.IsASink() {
					opts = append(opts, publish.IsSink())
				}
				toBuffers := v1alpha1.GenerateBufferNames(vertexInstance.Vertex.Namespace, pipelineName, e.To, e.GetToVertexPartitions())
				for _, buffer := range toBuffers {
					publishWatermark[buffer] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore), opts...)
				}
			}
		}
	}
	return fetchWatermark, publishWatermark, nil
}

// BuildSourcePublisherStores builds the watermark stores for source publisher.
func BuildSourcePublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (store.WatermarkStorer, error) {
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
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore for source, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	// OT
	otStoreBucketName := isbsvc.JetStreamOTBucket(bucketName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVStore for source, OTBucket: %s, %w", otStoreBucketName, err)
	}
	sourcePublishStores := store.BuildWatermarkStore(hbKVStore, otKVStore)
	return sourcePublishStores, nil
}

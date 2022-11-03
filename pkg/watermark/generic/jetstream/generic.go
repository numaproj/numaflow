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
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
)

// BuildWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// Fetcher has one-to-one relationship, whereas we have multiple publishers as the vertex can read only from one edge,
// and it can write to many.
// The function is used only when watermarking is enabled on the pipeline.
func BuildWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetch.Fetcher, map[string]publish.Publisher, error) {
	// if watermark is not enabled, use no-op.
	if !sharedutil.IsWatermarkEnabled() {
		fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertexInstance.Vertex.GetToBuffers()))
		return fetchWatermark, publishWatermark, nil
	}

	pipelineName := vertexInstance.Vertex.Spec.PipelineName

	// Fetcher creation, we have only 1 in buffer ATM
	var fetchWatermark fetch.Fetcher
	fromBuffer := vertexInstance.Vertex.GetFromBuffers()[0]
	hbBucketName := isbsvc.JetStreamProcessorBucket(pipelineName, fromBuffer.Name)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new HB KVJetStreamKVWatch, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	otBucketName := isbsvc.JetStreamOTBucket(pipelineName, fromBuffer.Name)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new OT KVJetStreamKVWatch, OTBucket: %s, %w", otBucketName, err)
	}

	if fromBuffer.Type == v1alpha1.SourceBuffer {
		fetchWatermark = generic.NewGenericSourceFetch(ctx, fromBuffer.Name, store.BuildWatermarkStoreWatcher(hbWatch, otWatch))
	} else {
		fetchWatermark = generic.NewGenericEdgeFetch(ctx, fromBuffer.Name, store.BuildWatermarkStoreWatcher(hbWatch, otWatch))
	}

	// Publisher map creation, we need a publisher per out edge.
	var publishWatermark = make(map[string]publish.Publisher)
	for _, buffer := range vertexInstance.Vertex.GetToBuffers() {
		hbPublisherBucketName := isbsvc.JetStreamProcessorBucket(pipelineName, buffer.Name)
		// We create a separate Heartbeat bucket for each edge though it can be reused. We can reuse because heartbeat is at
		// vertex level. We are creating a new one for the time being because controller creates a pair of buckets per edge.
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucketName, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucketName, err)
		}

		otStoreBucketName := isbsvc.JetStreamOTBucket(pipelineName, buffer.Name)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucketName, err)
		}

		var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
		publishEntity := processor.NewProcessorEntity(processorName)
		opts := []publish.PublishOption{}
		if buffer.Type == v1alpha1.SinkBuffer {
			opts = append(opts, publish.IsSink())
		}
		publishWatermark[buffer.Name] = publish.NewPublish(ctx, publishEntity, store.BuildWatermarkStore(hbStore, otStore), opts...)
	}

	return fetchWatermark, publishWatermark, nil
}

// BuildSourcePublisherStores builds the watermark stores for source publisher.
func BuildSourcePublisherStores(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (store.WatermarkStorer, error) {
	if !vertexInstance.Vertex.IsASource() {
		return nil, fmt.Errorf("not a source vertex")
	}
	if !sharedutil.IsWatermarkEnabled() {
		return store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore()), nil
	}
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	sourceBufferName := vertexInstance.Vertex.GetFromBuffers()[0].Name
	// heartbeat
	hbBucketName := isbsvc.JetStreamProcessorBucket(pipelineName, sourceBufferName)
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, fmt.Errorf("failed at new HB KVJetStreamKVStore for source, HeartbeatBucket: %s, %w", hbBucketName, err)
	}

	// OT
	otStoreBucketName := isbsvc.JetStreamOTBucket(pipelineName, sourceBufferName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucketName, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, fmt.Errorf("failed at new OT KVJetStreamKVStore for source, OTBucket: %s, %w", otStoreBucketName, err)
	}
	sourcePublishStores := store.BuildWatermarkStore(hbKVStore, otKVStore)
	return sourcePublishStores, nil
}

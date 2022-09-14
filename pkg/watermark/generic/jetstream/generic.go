// Package generic implements some shareable watermarking progressors (fetcher and publisher) and methods.

package jetstream

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
)

// BuildJetStreamWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// Fetcher has one-to-one relationship , whereas we have multiple publishers as the vertex can read only from one edge,
// and it can write to many.
// The function is used only when watermarking is enabled on the pipeline.
func BuildJetStreamWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetch.Fetcher, map[string]publish.Publisher, error) {
	// if watermark is not enabled, use no-op.
	if !sharedutil.IsWatermarkEnabled() {
		fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(vertexInstance.Vertex.GetToBuffers()))
		return fetchWatermark, publishWatermark, nil
	}

	publishWatermark := make(map[string]publish.Publisher)
	// Fetcher creation
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	fromBuffer := vertexInstance.Vertex.GetFromBuffers()[0]
	hbBucket := isbsvc.JetStreamProcessorBucket(pipelineName, fromBuffer.Name)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucket, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new HB KVJetStreamKVWatch, HeartbeatBucket: %s, %w", hbBucket, err)
	}

	otBucket := isbsvc.JetStreamOTBucket(pipelineName, fromBuffer.Name)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucket, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, fmt.Errorf("failed at new OT KVJetStreamKVWatch, OTBucket: %s, %w", otBucket, err)
	}

	var fetchWmWatchers = generic.BuildFetchWMWatchers(hbWatch, otWatch)
	var fetchWatermark fetch.Fetcher
	if fromBuffer.Type == v1alpha1.SourceBuffer {
		fetchWatermark = generic.NewGenericSourceFetch(ctx, fromBuffer.Name, fetchWmWatchers)
	} else {
		fetchWatermark = generic.NewGenericEdgeFetch(ctx, fromBuffer.Name, fetchWmWatchers)
	}

	// Publisher map creation, we need a publisher per edge.

	for _, buffer := range vertexInstance.Vertex.GetToBuffers() {
		hbPublisherBucket := isbsvc.JetStreamProcessorBucket(pipelineName, buffer.Name)
		// We create a separate Heartbeat bucket for each edge though it can be reused. We can reuse because heartbeat is at
		// vertex level. We are creating a new one for the time being because controller creates a pair of buckets per edge.
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucket, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new HB Publish JetStreamKVStore, HeartbeatPublisherBucket: %s, %w", hbPublisherBucket, err)
		}

		otStoreBucket := isbsvc.JetStreamOTBucket(pipelineName, buffer.Name)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucket, jsclient.NewInClusterJetStreamClient())
		if err != nil {
			return nil, nil, fmt.Errorf("failed at new OT Publish JetStreamKVStore, OTBucket: %s, %w", otStoreBucket, err)
		}

		var publishStores = generic.BuildPublishWMStores(hbStore, otStore)
		var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
		publishWatermark[buffer.Name] = generic.NewGenericPublish(ctx, processorName, *publishStores)
	}

	return fetchWatermark, publishWatermark, nil
}

// BuildJetStreamWatermarkProgressorsForSource is an extension of BuildJetStreamWatermarkProgressors to also return the publish stores. This is
// for letting source implement as many publishers that it requires to progress the watermark monotonically for each individual processing entity.
// Eg, watermark progresses independently and monotonically for each partition in a Kafka topic.
func BuildJetStreamWatermarkProgressorsForSource(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetch.Fetcher, map[string]publish.Publisher, *generic.PublishWMStores, error) {
	if vertexInstance == nil || vertexInstance.Vertex == nil {
		return nil, nil, nil, fmt.Errorf("nil vertex instance")
	}
	if !vertexInstance.Vertex.IsASource() {
		return nil, nil, nil, fmt.Errorf("invalid vertex: not a source")
	}
	fetchWatermark, publishWatermark, err := BuildJetStreamWatermarkProgressors(ctx, vertexInstance)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to build JetStreamWatermarkProgressors, %w", err)
	}
	// return no-ops if not enabled!f
	if !sharedutil.IsWatermarkEnabled() {
		publishWM := generic.BuildPublishWMStores(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
		return fetchWatermark, publishWatermark, publishWM, nil
	}

	pipelineName := vertexInstance.Vertex.Spec.PipelineName

	sourceBufferName := vertexInstance.Vertex.GetFromBuffers()[0].Name
	// heartbeat
	hbBucket := isbsvc.JetStreamProcessorBucket(pipelineName, sourceBufferName)
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucket, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed at new HB KVJetStreamKVStore for source, HeartbeatBucket: %s, %w", hbBucket, err)
	}

	// OT
	otStoreBucket := isbsvc.JetStreamOTBucket(pipelineName, sourceBufferName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucket, jsclient.NewInClusterJetStreamClient())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed at new OT KVJetStreamKVStore for source, OTBucket: %s, %w", otStoreBucket, err)
	}

	// interface for publisher store (HB and OT)
	publishWM := generic.BuildPublishWMStores(hbKVStore, otKVStore)
	return fetchWatermark, publishWatermark, publishWM, nil
}

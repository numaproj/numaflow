// Package generic implements some shareable watermarking progressors (fetcher and publisher) and methods.

package generic

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"go.uber.org/zap"
)

// GetFetchKeyspace gets the fetch keyspace name fromEdge the vertex.
func GetFetchKeyspace(v *dfv1.Vertex) string {
	if len(v.Spec.FromEdges) > 0 {
		// fromEdge vertices is 0 because we do not support diamond DAG
		return fmt.Sprintf("%s-%s-%s", v.Namespace, v.Spec.PipelineName, v.Spec.FromEdges[0].From)
	} else {
		// sources will not have FromVertices
		return fmt.Sprintf("%s-%s-%s-source", v.Namespace, v.Spec.PipelineName, v.Name)
	}
}

// GetPublishKeySpace gets the publish keyspace name fromEdge the vertex
func GetPublishKeySpace(v *dfv1.Vertex) string {
	if v.IsASource() {
		return dfv1.GenerateSourceBufferName(v.Namespace, v.Spec.PipelineName, v.Spec.Name)
	} else {
		return fmt.Sprintf("%s-%s-%s", v.Namespace, v.Spec.PipelineName, v.Spec.Name)
	}
}

// BuildJetStreamWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// Fetcher has one-to-one relationship , whereas we have multiple publishers as the vertex can read only from one edge,
// and it can write to many.
// The function is used only when watermarking is enabled on the pipeline.
func BuildJetStreamWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher) {
	// if watermark is not enabled, use no-op.
	if !sharedutil.IsWatermarkEnabled() {
		fetchWatermark = NewNoOpWMProgressor()
		publishWatermark = make(map[string]publish.Publisher)
		for _, buffer := range vertexInstance.Vertex.GetToBuffers() {
			publishWatermark[buffer.Name] = NewNoOpWMProgressor()
		}

		return fetchWatermark, publishWatermark
	}

	log := logging.FromContext(ctx)
	publishWatermark = make(map[string]publish.Publisher)
	// Fetcher creation
	pipelineName := vertexInstance.Vertex.Spec.PipelineName
	fromBufferName := vertexInstance.Vertex.GetFromBuffers()[0].Name
	hbBucket := isbsvc.JetStreamProcessorBucket(pipelineName, fromBufferName)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucket, clients.NewInClusterJetStreamClient())
	if err != nil {
		log.Fatalw("JetStreamKVWatch failed", zap.String("HeartbeatBucket", hbBucket), zap.Error(err))
	}

	otBucket := isbsvc.JetStreamOTBucket(pipelineName, fromBufferName)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucket, clients.NewInClusterJetStreamClient())
	if err != nil {
		log.Fatalw("JetStreamKVWatch failed", zap.String("OTBucket", otBucket), zap.Error(err))
	}

	var fetchWmWatchers = BuildFetchWMWatchers(hbWatch, otWatch)
	fetchWatermark = NewGenericFetch(ctx, vertexInstance.Vertex.Name, fetchWmWatchers)

	// Publisher map creation, we need a publisher per edge.

	for _, buffer := range vertexInstance.Vertex.GetToBuffers() {
		hbPublisherBucket := isbsvc.JetStreamProcessorBucket(pipelineName, buffer.Name)
		// We create a separate Heartbeat bucket for each edge though it can be reused. We can reuse because heartbeat is at
		// vertex level. We are creating a new one for the time being because controller creates a pair of buckets per edge.
		hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbPublisherBucket, clients.NewInClusterJetStreamClient())
		if err != nil {
			log.Fatalw("JetStreamKVStore failed", zap.String("HeartbeatPublisherBucket", hbPublisherBucket), zap.Error(err))
		}

		otStoreBucket := isbsvc.JetStreamOTBucket(pipelineName, buffer.Name)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucket, clients.NewInClusterJetStreamClient())
		if err != nil {
			log.Fatalw("JetStreamKVStore failed", zap.String("OTBucket", otStoreBucket), zap.Error(err))
		}

		var publishStores = BuildPublishWMStores(hbStore, otStore)
		var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
		publishWatermark[buffer.Name] = NewGenericPublish(ctx, processorName, publishStores)
	}

	return fetchWatermark, publishWatermark
}

// BuildJetStreamWatermarkProgressorsForSource is an extension of BuildJetStreamWatermarkProgressors to also return the publish stores. This is
// for letting source implement as many publishers that it requires to progress the watermark monotonically for each individual processing entity.
// Eg, watermark progresses independently and monotonically for each partition in a Kafka topic.
func BuildJetStreamWatermarkProgressorsForSource(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, publishWM PublishWMStores) {
	fetchWatermark, publishWatermark = BuildJetStreamWatermarkProgressors(ctx, vertexInstance)

	// return no-ops if not enabled!
	if !sharedutil.IsWatermarkEnabled() {
		publishWM = BuildPublishWMStores(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
		return fetchWatermark, publishWatermark, publishWM
	}

	log := logging.FromContext(ctx)
	pipelineName := vertexInstance.Vertex.Spec.PipelineName

	sourceBufferName := vertexInstance.Vertex.GetFromBuffers()[0].Name
	// hearbeat
	hbBucket := isbsvc.JetStreamProcessorBucket(pipelineName, sourceBufferName)
	hbKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, hbBucket, clients.NewInClusterJetStreamClient())
	if err != nil {
		log.Fatalw("JetStreamKVStore failed", zap.String("HeartbeatBucket", hbBucket), zap.Error(err))
	}

	// OT
	otStoreBucket := isbsvc.JetStreamOTBucket(pipelineName, sourceBufferName)
	otKVStore, err := jetstream.NewKVJetStreamKVStore(ctx, pipelineName, otStoreBucket, clients.NewInClusterJetStreamClient())
	if err != nil {
		log.Fatalw("JetStreamKVStore failed", zap.String("OTBucket", otStoreBucket), zap.Error(err))
	}

	// interface for publisher store (HB and OT)
	publishWM = BuildPublishWMStores(hbKVStore, otKVStore)

	return fetchWatermark, publishWatermark, publishWM
}

package generic

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"go.uber.org/zap"
)

type genericProgressOptions struct {
	separateOTBucket bool
}

// GenericProgress implements `Progressor` to generic the watermark for UDFs and Sinks.
type GenericProgress struct {
	progressPublish *publish.Publish
	progressFetch   *fetch.Edge
	opts            *genericProgressOptions
}

var _ Progressor = (*GenericProgress)(nil)

// GenericProgressOption sets options for GenericProgress.
type GenericProgressOption func(options *genericProgressOptions)

// WithSeparateOTBuckets creates a different bucket for maintaining each processor offset-timeline.
func WithSeparateOTBuckets(separate bool) GenericProgressOption {
	return func(opts *genericProgressOptions) {
		opts.separateOTBucket = separate
	}
}

// NewGenericProgress will move the watermark for all the vertices once consumed fromEdge the source.
func NewGenericProgress(ctx context.Context, processorName string, fetchKeyspace string, publishKeyspace string, publishWM PublishWM, fetchWM FetchWM, inputOpts ...GenericProgressOption) *GenericProgress {
	opts := &genericProgressOptions{
		separateOTBucket: false,
	}

	for _, opt := range inputOpts {
		opt(opts)
	}

	var log = logging.FromContext(ctx)
	_ = log
	// to generic watermark for a UDF, it has to start the Fetcher and the Publisher

	publishEntity := processor.NewProcessorEntity(processorName, publishKeyspace)
	udfPublish := publish.NewPublish(ctx, publishEntity, publishWM.hbStore, publishWM.otStore)

	udfFromVertex := fetch.NewFromVertex(ctx, fetchKeyspace, fetchWM.hbWatch, fetchWM.otWatch)
	udfFetch := fetch.NewEdgeBuffer(ctx, processorName, udfFromVertex)

	u := &GenericProgress{
		progressPublish: udfPublish,
		progressFetch:   udfFetch,
		opts:            opts,
	}

	return u
}

// GetWatermark gets the watermark.
func (u *GenericProgress) GetWatermark(offset isb.Offset) processor.Watermark {
	return u.progressFetch.GetWatermark(offset)
}

// PublishWatermark publishes the watermark.
func (u *GenericProgress) PublishWatermark(watermark processor.Watermark, offset isb.Offset) {
	u.progressPublish.PublishWatermark(watermark, offset)
}

// GetLatestWatermark returns the latest head watermark.
func (u *GenericProgress) GetLatestWatermark() processor.Watermark {
	return u.progressPublish.GetLatestWatermark()
}

func (g *GenericProgress) StopPublisher() {
	g.progressPublish.StopPublisher()
}

// BuildJetStreamWatermarkProgressors is used to populate fetchWatermark, and a map of publishWatermark with edge name as the key.
// These are used as watermark progressors in the pipeline, and is attached to each edge of the vertex.
// Fetcher has one-to-one relationship , whereas we have multiple publishers as the vertex can read only from one edge,
// and it can write to many.
// The function is used only when watermarking is enabled on the pipeline.
func BuildJetStreamWatermarkProgressors(ctx context.Context, vertexInstance *v1alpha1.VertexInstance) (fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher) {

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

	var fetchWmWatchers = BuildFetchWM(hbWatch, otWatch)
	fetchWatermark = NewGenericFetch(ctx, vertexInstance.Vertex.Name, GetFetchKeyspace(vertexInstance.Vertex), fetchWmWatchers)

	// Publisher map creation
	// We do not separate Heartbeat bucket for each edge, can be reused
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, vertexInstance.Vertex.Spec.PipelineName, hbBucket, clients.NewInClusterJetStreamClient())
	if err != nil {
		log.Fatalw("JetStreamKVStore failed", zap.String("HeartbeatBucket", hbBucket), zap.Error(err))
	}

	for _, buffer := range vertexInstance.Vertex.GetToBuffers() {
		var processorName = fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
		streamName := isbsvc.JetStreamName(pipelineName, buffer.Name)
		otStoreBucket := isbsvc.JetStreamOTBucket(pipelineName, streamName)
		otStore, err := jetstream.NewKVJetStreamKVStore(ctx, vertexInstance.Vertex.Spec.PipelineName, otStoreBucket, clients.NewInClusterJetStreamClient())
		if err != nil {
			log.Fatalw("JetStreamKVStore failed", zap.String("OTBucket", otStoreBucket), zap.Error(err))
		}
		var publishStores = BuildPublishWM(hbStore, otStore)
		publishWatermark[streamName] = NewGenericPublish(ctx, processorName, GetPublishKeySpace(vertexInstance.Vertex), publishStores)
	}

	return fetchWatermark, publishWatermark
}

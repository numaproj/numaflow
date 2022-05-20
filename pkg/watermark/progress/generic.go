package progress

import (
	"context"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

type genericProgressOptions struct {
	separateOTBucket bool
}

// GenericProgress implements `Progressor` to progress the watermark for UDFs and Sinks.
type GenericProgress struct {
	progressPublish *publish.Publish
	progressFetch   *fetch.EdgeBuffer
	opts            *genericProgressOptions
}

// GenericProgressOption sets options for GenericProgress.
type GenericProgressOption func(options *genericProgressOptions)

// WithSeparateOTBuckets creates a different bucket for maintaining each processor offset-timeline.
func WithSeparateOTBuckets(separate bool) GenericProgressOption {
	return func(opts *genericProgressOptions) {
		opts.separateOTBucket = separate
	}
}

// NewGenericProgress will move the watermark for all the UDF vertices.
func NewGenericProgress(ctx context.Context, processorName string, fetchKeyspace string, publishKeyspace string, js nats.JetStreamContext, inputOpts ...GenericProgressOption) *GenericProgress {
	var log = logging.FromContext(ctx)

	opts := &genericProgressOptions{
		separateOTBucket: false,
	}

	for _, opt := range inputOpts {
		opt(opts)
	}

	// to progress watermark for a UDF, it has to start the Fetcher and the Publisher

	// publish
	publishEntity := processor.NewProcessorEntity(processorName, publishKeyspace, processor.WithSeparateOTBuckets(opts.separateOTBucket))
	publishHeartbeatBucket, err := js.KeyValue(publishKeyspace + "_PROCESSORS")
	if err != nil {
		log.Fatalw("unable to get the publish heartbeat bucket", zap.String("bucket", publishKeyspace+"_PROCESSORS"), zap.Error(err))
	}
	udfPublish := publish.NewPublish(ctx, publishEntity, publishKeyspace, js, publishHeartbeatBucket, publish.WithAutoRefreshHeartbeat(true))

	// fetch
	fetchHeartbeatBucket, err := js.KeyValue(fetchKeyspace + "_PROCESSORS")
	if err != nil {
		log.Fatalw("unable to get the fetch heartbeat bucket", zap.String("bucket", fetchKeyspace+"_PROCESSORS"), zap.Error(err))
	}
	heartbeatWatcher, err := fetchHeartbeatBucket.WatchAll()
	if err != nil {
		log.Fatalw("unable to create the fetch heartbeat bucket watcher", zap.String("bucket", fetchKeyspace+"_PROCESSORS"), zap.Error(err))
	}
	udfFromVertex := fetch.NewFromVertex(ctx, fetchKeyspace, js, heartbeatWatcher, fetch.WithSeparateOTBuckets(opts.separateOTBucket))
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

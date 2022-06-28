package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
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

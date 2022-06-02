package generator

import (
	"fmt"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sources/types"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/progress"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// buildWMProgressor builds the watermark progressor. It will create a sourcePublisher so it can publish the watermark
// it reads from the source. The sourcePublisher is passed in as the input for generic progressor.
func (mg *memgen) buildWMProgressor(metadata *types.SourceMetadata) error {
	ctx := mg.lifecycleCtx

	js, err := progress.GetJetStreamConnection(mg.lifecycleCtx)
	if err != nil {
		return err
	}

	// publish source watermark and this is very much dependent on the source
	sourcePublishKeySpace := fmt.Sprintf("source-%s", progress.GetPublishKeySpace(metadata.Vertex))
	// TODO: remove this once bucket creation has been moved to controller
	err = progress.CreateProcessorBucketIfMissing(fmt.Sprintf("%s_PROCESSORS", sourcePublishKeySpace), js)
	if err != nil {
		return err
	}
	publishEntity := processor.NewProcessorEntity(fmt.Sprintf("source-%s-%d", metadata.Vertex.Name, metadata.Replica), sourcePublishKeySpace, processor.WithSeparateOTBuckets(false))
	// for tickgen you need the default heartbeat system because there are no concept of source partitions etc
	heartbeatBucket, err := progress.GetHeartbeatBucket(js, sourcePublishKeySpace)
	if err != nil {
		return err
	}
	// use this while reading the data from the source.
	mg.progressor.sourcePublish = publish.NewPublish(mg.lifecycleCtx, publishEntity, progress.GetPublishKeySpace(metadata.Vertex), js, heartbeatBucket)

	// fall back on the generic progressor and use the source publisher as the input to the generic progressor.
	// use the source Publisher as the source

	// TODO: remove this once bucket creation has been moved to controller
	err = progress.CreateProcessorBucketIfMissing(fmt.Sprintf("%s_PROCESSORS", progress.GetPublishKeySpace(metadata.Vertex)), js)
	if err != nil {
		return err
	}
	var wmProgressor = progress.NewGenericProgress(ctx, fmt.Sprintf("%s-%d", metadata.Vertex.Name, metadata.Replica), sourcePublishKeySpace, progress.GetPublishKeySpace(metadata.Vertex), js)
	mg.progressor.wmProgressor = wmProgressor

	mg.logger.Info("Initialized watermark progressor")

	return nil
}

func (w *watermark) PublishWatermark(watermark processor.Watermark, offset isb.Offset) {
	w.wmProgressor.PublishWatermark(watermark, offset)
}

func (w *watermark) GetLatestWatermark() processor.Watermark {
	return w.wmProgressor.GetLatestWatermark()
}

func (w *watermark) StopPublisher() {
	w.wmProgressor.StopPublisher()
}

func (w *watermark) GetWatermark(offset isb.Offset) processor.Watermark {
	return w.wmProgressor.GetWatermark(offset)
}

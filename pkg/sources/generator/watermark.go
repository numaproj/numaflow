package generator

import (
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// buildWMProgressor builds the watermark progressor. It will create a sourcePublisher so it can publish the watermark
// it reads from the source. The sourcePublisher is passed in as the input for generic progressor.
func (mg *memgen) buildWMProgressor(vertexInstance *dfv1.VertexInstance) error {
	ctx := mg.lifecycleCtx

	js, err := generic.GetJetStreamConnection(mg.lifecycleCtx)
	if err != nil {
		return err
	}

	// publish source watermark and this is very much dependent on the source
	sourcePublishKeySpace := fmt.Sprintf("source-%s", generic.GetPublishKeySpace(vertexInstance.Vertex))
	// TODO: remove this once bucket creation has been moved to controller
	err = generic.CreateProcessorBucketIfMissing(fmt.Sprintf("%s_PROCESSORS", sourcePublishKeySpace), js)
	if err != nil {
		return err
	}
	publishEntity := processor.NewProcessorEntity(fmt.Sprintf("source-%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica), sourcePublishKeySpace, processor.WithSeparateOTBuckets(false))

	if err != nil {
		return err
	}
	// use this while reading the data from the source.
	mg.progressor.sourcePublish = publish.NewPublish(mg.lifecycleCtx, publishEntity, nil, nil)

	// fall back on the generic progressor and use the source publisher as the input to the generic progressor.
	// use the source Publisher as the source

	// TODO: remove this once bucket creation has been moved to controller
	err = generic.CreateProcessorBucketIfMissing(fmt.Sprintf("%s_PROCESSORS", generic.GetPublishKeySpace(vertexInstance.Vertex)), js)
	if err != nil {
		return err
	}
	var fetchWM = generic.BuildFetchWMWatchers(nil, nil)
	var publishWM = generic.BuildPublishWMStores(nil, nil)
	var wmProgressor = generic.NewGenericProgress(ctx, fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica), sourcePublishKeySpace, generic.GetPublishKeySpace(vertexInstance.Vertex), publishWM, fetchWM)
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

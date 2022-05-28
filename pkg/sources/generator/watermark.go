package generator

import (
	"fmt"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sources/types"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/progress"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

func (mg *memgen) buildWMProgressor(metadata *types.SourceMetadata) (progress.Progressor, error) {
	js, err := progress.GetJetStreamConnection(mg.lifecycleCtx)
	if err != nil {
		return nil, err
	}
	publishEntity := processor.NewProcessorEntity(fmt.Sprintf("%s-%d", metadata.Vertex.Name, metadata.Replica), progress.GetPublishKeySpace(metadata.Vertex), processor.WithSeparateOTBuckets(false))
	heartbeatBucket, err := progress.GetHeartbeatBucket(js, progress.GetPublishKeySpace(metadata.Vertex))
	if err != nil {
		return nil, err
	}

	w := watermark{}

	w.publisher = publish.NewPublish(mg.lifecycleCtx, publishEntity, progress.GetPublishKeySpace(metadata.Vertex), js, heartbeatBucket)

	return &w, nil
}

func (w *watermark) PublishWatermark(watermark processor.Watermark, offset isb.Offset) {
	w.publisher.PublishWatermark(watermark, offset)
}

func (w *watermark) GetLatestWatermark() processor.Watermark {
	return w.publisher.GetLatestWatermark()
}

func (w *watermark) StopPublisher() {
	w.publisher.StopPublisher()
}

func (w *watermark) GetWatermark(offset isb.Offset) processor.Watermark {
	//TODO implement me
	panic("implement me")
}

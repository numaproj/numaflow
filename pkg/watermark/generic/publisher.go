package generic

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// PublishWMStores stores the store information for publishing the watermark.
type PublishWMStores struct {
	hbStore store.WatermarkKVStorer
	otStore store.WatermarkKVStorer
}

// BuildPublishWMStores builds the PublishWMStores.
func BuildPublishWMStores(hbStore store.WatermarkKVStorer, otStore store.WatermarkKVStorer) PublishWMStores {
	return PublishWMStores{
		hbStore: hbStore,
		otStore: otStore,
	}
}

// GenericPublish is a generic publisher which will work for most cases.
type GenericPublish struct {
	toEdge *publish.Publish
}

var _ publish.Publisher = (*GenericPublish)(nil)

// NewGenericPublish returns GenericPublish. processorName is the unique processor (pod) that is running on this vertex.
// publishKeyspace is obsolete, and will be removed in subsequent iterations. publishWM is a struct for storing both the heartbeat
// and the offset watermark timeline stores for the Vn vertex.
func NewGenericPublish(ctx context.Context, processorName string, publishKeyspace string, publishWM PublishWMStores) *GenericPublish {
	publishEntity := processor.NewProcessorEntity(processorName, publishKeyspace)
	udfPublish := publish.NewPublish(ctx, publishEntity, publishWM.hbStore, publishWM.otStore)
	gp := &GenericPublish{
		toEdge: udfPublish,
	}
	return gp
}

// PublishWatermark publishes for the generic publisher.
func (g *GenericPublish) PublishWatermark(watermark processor.Watermark, offset isb.Offset) {
	g.toEdge.PublishWatermark(watermark, offset)
}

// GetLatestWatermark gets the latest watermakr for the generic publisher.
func (g *GenericPublish) GetLatestWatermark() processor.Watermark {
	return g.toEdge.GetLatestWatermark()
}

// StopPublisher stops the generic publisher.
func (g *GenericPublish) StopPublisher() {
	g.toEdge.StopPublisher()
}

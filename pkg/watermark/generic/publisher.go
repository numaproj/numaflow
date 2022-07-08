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
	HBStore store.WatermarkKVStorer
	OTStore store.WatermarkKVStorer
}

// BuildPublishWMStores builds the PublishWMStores.
func BuildPublishWMStores(hbStore store.WatermarkKVStorer, otStore store.WatermarkKVStorer) PublishWMStores {
	return PublishWMStores{
		HBStore: hbStore,
		OTStore: otStore,
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
func NewGenericPublish(ctx context.Context, processorName string, publishWM PublishWMStores) *GenericPublish {
	publishEntity := processor.NewProcessorEntity(processorName)
	udfPublish := publish.NewPublish(ctx, publishEntity, publishWM.HBStore, publishWM.OTStore)
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

package generic

import (
	"context"

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
func BuildPublishWMStores(hbStore store.WatermarkKVStorer, otStore store.WatermarkKVStorer) *PublishWMStores {
	return &PublishWMStores{
		HBStore: hbStore,
		OTStore: otStore,
	}
}

// NewGenericPublish returns GenericPublish. processorName is the unique processor (pod) that is running on this vertex.
// publishKeyspace is obsolete, and will be removed in subsequent iterations. publishWM is a struct for storing both the heartbeat
// and the offset watermark timeline stores for the Vn vertex.
func NewGenericPublish(ctx context.Context, processorName string, publishWM PublishWMStores) publish.Publisher {
	publishEntity := processor.NewProcessorEntity(processorName)
	return publish.NewPublish(ctx, publishEntity, publishWM.HBStore, publishWM.OTStore)
}

package generator

import (
	"fmt"

	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

func (mg *memgen) buildSourceWatermarkPublisher(publishWMStores *generic.PublishWMStores) publish.Publisher {
	// for tickgen, it can be the name of the replica
	entityName := fmt.Sprintf("%s-%d", mg.vertexInstance.Vertex.Name, mg.vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	return publish.NewPublish(mg.lifecycleCtx, processorEntity, publishWMStores.HBStore, publishWMStores.OTStore, publish.WithDelay(sharedutil.GetWatermarkMaxDelay()))
}

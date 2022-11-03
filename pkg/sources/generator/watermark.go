/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package generator

import (
	"fmt"

	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

func (mg *memgen) buildSourceWatermarkPublisher(publishWMStores store.WatermarkStorer) publish.Publisher {
	// for tickgen, it can be the name of the replica
	entityName := fmt.Sprintf("%s-%d", mg.vertexInstance.Vertex.Name, mg.vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	return publish.NewPublish(mg.lifecycleCtx, processorEntity, publishWMStores, publish.IsSource(), publish.WithDelay(sharedutil.GetWatermarkMaxDelay()))
}

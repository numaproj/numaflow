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

package idlehandler

import (
	"context"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// PublishIdleWatermark publishes a ctrl message with isb.Kind set to WMB. We only send one ctrl message when
func PublishIdleWatermark(ctx context.Context, toBufferPartition isb.BufferWriter, wmPublisher publish.Publisher, idleManager wmb.IdleManager, logger *zap.SugaredLogger, vertexType dfv1.VertexType, wm wmb.Watermark) {

	var toPartitionName = toBufferPartition.GetName()
	var toVertexPartition = toBufferPartition.GetPartitionIdx()

	if idleManager.NeedToSendCtrlMsg(toPartitionName) {
		if vertexType == dfv1.VertexTypeSink {
			// for Sink vertex, we don't need to write any ctrl message
			// and because when we publish the watermark, offset is not important for sink
			// so, we do nothing here
		} else { // if the toBuffer partition doesn't exist, then we get a new idle situation
			// if wmbOffset is nil, create a new WMB and write a ctrl message to ISB
			var ctrlMessage = []isb.Message{{Header: isb.Header{Kind: isb.WMB}}}
			writeOffsets, errs := toBufferPartition.Write(ctx, ctrlMessage)
			// we only write one ctrl message, so there's one and only one error in the array, use index=0 to get the error
			if errs[0] != nil {
				logger.Errorw("Failed to write ctrl message to buffer", zap.String("toPartitionName", toPartitionName), zap.Error(errs[0]))
				return
			}
			logger.Debug("Succeeded to write ctrl message to buffer", zap.String("toPartitionName", toPartitionName), zap.Error(errs[0]))

			if len(writeOffsets) == 1 {
				// we only write one ctrl message, so there's only one offset in the array, use index=0 to get the offset
				idleManager.Update(toPartitionName, writeOffsets[0])
			}
		}
	}

	// publish WMB (this will naturally incr or set the timestamp of rl.wmbOffset)
	if vertexType == dfv1.VertexTypeSource || vertexType == dfv1.VertexTypeMapUDF ||
		vertexType == dfv1.VertexTypeReduceUDF {
		wmPublisher.PublishIdleWatermark(wm, idleManager.Get(toPartitionName), toVertexPartition)
	} else {
		// for Sink vertex, and it does not care about the offset during watermark publishing
		wmPublisher.PublishIdleWatermark(wm, nil, toVertexPartition)
	}
}

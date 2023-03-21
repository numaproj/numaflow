package idlehandler

import (
	"context"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"go.uber.org/zap"
)

// PublishIdleWatermark publishes a ctrl message with isb.Kind set to WMB. We only send one ctrl message when
// we see a new WMB; later we only update the WMB without a ctrl message.
func PublishIdleWatermark(ctx context.Context, toBuffer isb.BufferWriter, publisher publish.Publisher, idleManager *wmb.IdleManager, logger *zap.SugaredLogger, vertexType dfv1.VertexType, wm wmb.Watermark) {
	var bufferName = toBuffer.GetName()

	if !idleManager.Exists(bufferName) {
		if vertexType == dfv1.VertexTypeSink {
			// for Sink vertex, we don't need to write any ctrl message
			// and because when we publish the watermark, offset is not important for sink
			// so, we do nothing here
		} else { // if the buffer doesn't exist, then we get a new idle situation
			// if wmbOffset is nil, create a new WMB and write a ctrl message to ISB
			var ctrlMessage = []isb.Message{{Header: isb.Header{Kind: isb.WMB}}}
			writeOffsets, errs := toBuffer.Write(ctx, ctrlMessage)
			// we only write one ctrl message, so there's one and only one error in the array, use index=0 to get the error
			if errs[0] != nil {
				logger.Errorw("failed to write ctrl message to buffer", zap.String("bufferName", bufferName), zap.Error(errs[0]))
				return
			}
			logger.Debug("succeeded to write ctrl message to buffer", zap.String("bufferName", bufferName), zap.Error(errs[0]))

			if len(writeOffsets) == 1 {
				// we only write one ctrl message, so there's only one offset in the array, use index=0 to get the offset
				idleManager.Update(bufferName, writeOffsets[0])
			}
		}
	}

	// publish WMB (this will naturally incr or set the timestamp of rl.wmbOffset)
	if vertexType == dfv1.VertexTypeSource || vertexType == dfv1.VertexTypeMapUDF ||
		vertexType == dfv1.VertexTypeReduceUDF {
		publisher.PublishIdleWatermark(wm, idleManager.Get(bufferName))
	} else {
		// for Sink vertex, and it does not care about the offset during watermark publishing
		publisher.PublishIdleWatermark(wm, nil)
	}
}

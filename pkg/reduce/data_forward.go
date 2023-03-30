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

// Package reduce reads messages from isb
// attaches watermark to read messages
// invoke the read-loop with the read messages
package reduce

import (
	"context"
	"strconv"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/readloop"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
)

// DataForward reads data from isb and forwards them to readloop
type DataForward struct {
	ctx                 context.Context
	vertexName          string
	pipelineName        string
	vertexReplica       int32
	fromBuffer          isb.BufferReader
	toBuffers           map[string]isb.BufferWriter
	readloop            *readloop.ReadLoop
	watermarkFetcher    fetch.Fetcher
	watermarkPublishers map[string]publish.Publisher
	windowingStrategy   window.Windower
	keyed               bool
	idleManager         *wmb.IdleManager
	wmbChecker          wmb.WMBChecker
	pbqManager          *pbq.Manager
	opts                *Options
	log                 *zap.SugaredLogger
}

func NewDataForward(ctx context.Context,
	udf applier.ReduceApplier,
	vertexInstance *dfv1.VertexInstance,
	fromBuffer isb.BufferReader,
	toBuffers map[string]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forward.ToWhichStepDecider,
	fw fetch.Fetcher,
	watermarkPublishers map[string]publish.Publisher,
	windowingStrategy window.Windower,
	opts ...Option) (*DataForward, error) {

	options := DefaultOptions()

	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	idleManager := wmb.NewIdleManager(len(toBuffers))

	rl, err := readloop.NewReadLoop(ctx, vertexInstance.Vertex.Spec.Name, vertexInstance.Vertex.Spec.PipelineName,
		vertexInstance.Replica, udf, pbqManager, windowingStrategy, toBuffers, whereToDecider, watermarkPublishers, idleManager)

	df := &DataForward{
		ctx:                 ctx,
		vertexName:          vertexInstance.Vertex.Spec.Name,
		pipelineName:        vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:       vertexInstance.Replica,
		fromBuffer:          fromBuffer,
		toBuffers:           toBuffers,
		readloop:            rl,
		watermarkFetcher:    fw,
		watermarkPublishers: watermarkPublishers,
		windowingStrategy:   windowingStrategy,
		keyed:               vertexInstance.Vertex.Spec.UDF.GroupBy.Keyed,
		idleManager:         idleManager,
		pbqManager:          pbqManager,
		wmbChecker:          wmb.NewWMBChecker(2), // TODO: make configurable
		log:                 logging.FromContext(ctx),
		opts:                options}

	return df, err
}

// Start starts forwarding messages to readloop
func (d *DataForward) Start() {
	for {
		select {
		case <-d.ctx.Done():
			d.log.Infow("Stopping reduce data forwarder...")

			if err := d.fromBuffer.Close(); err != nil {
				d.log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
			} else {
				d.log.Infow("Closed buffer reader", zap.String("bufferFrom", d.fromBuffer.GetName()))
			}

			// hard shutdown after timeout
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// allow readloop to clean itself up.
			d.readloop.ShutDown(cctx)

			for _, v := range d.toBuffers {
				if err := v.Close(); err != nil {
					d.log.Errorw("Failed to close buffer writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", v.GetName()))
				} else {
					d.log.Infow("Closed buffer writer", zap.String("bufferTo", v.GetName()))
				}
			}

			// stop watermark fetcher
			if err := d.watermarkFetcher.Close(); err != nil {
				d.log.Errorw("Failed to close watermark fetcher", zap.Error(err))
			}

			// stop watermark publisher
			for _, publisher := range d.watermarkPublishers {
				if err := publisher.Close(); err != nil {
					d.log.Errorw("Failed to close watermark publisher", zap.Error(err))
				}
			}
			return
		default:
			// pass the child context so that the reader can be closed before the readloop
			// this way we can avoid the race condition and have all the read messages persisted
			// and acked.
			d.forwardAChunk(d.ctx)
		}
	}
}

// forwardAChunk reads a chunk of messages from isb and assigns watermark to messages
// and forwards the messages to readloop
func (d *DataForward) forwardAChunk(ctx context.Context) {
	readMessages, err := d.fromBuffer.Read(ctx, d.opts.readBatchSize)
	totalBytes := 0
	if err != nil {
		d.log.Errorw("Failed to read from isb", zap.Error(err))
		readMessagesError.With(map[string]string{
			metrics.LabelVertex:             d.vertexName,
			metrics.LabelPipeline:           d.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(d.vertexReplica)),
			"buffer":                        d.fromBuffer.GetName()}).Inc()
	}

	if len(readMessages) == 0 {
		// we use the HeadWMB as the watermark for the idle
		var processorWMB = d.watermarkFetcher.GetHeadWMB()
		if !d.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			d.log.Debugw("skip publishing idle watermark",
				zap.Int("counter", d.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("Idle", processorWMB.Idle))
			return
		}

		nextWin := d.pbqManager.NextWindowToBeClosed()
		if nextWin == nil {
			// if all the windows are closed already, and the len(readBatch) == 0
			// then it means there's an idle situation
			// in this case, send idle watermark to all the toBuffers
			for _, toBuffer := range d.toBuffers {
				if publisher, ok := d.watermarkPublishers[toBuffer.GetName()]; ok {
					idlehandler.PublishIdleWatermark(ctx, toBuffer, publisher, d.idleManager, d.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
				}
			}
		} else {
			// if toBeClosed window exists, and the watermark we fetch has already passed the endTime of this window
			// then it means the overall dataflow of the pipeline has already reached a later time point
			// so we can close the window and process the data in this window
			if watermark := time.UnixMilli(processorWMB.Watermark).Add(-1 * time.Millisecond); nextWin.EndTime().Before(watermark) {
				closedWindows := d.windowingStrategy.RemoveWindows(watermark)
				for _, win := range closedWindows {
					d.readloop.ClosePartitions(win.Partitions())
				}
			} else {
				// if toBeClosed window exists, but the watermark we fetch is still within the endTime of the window
				// then we can't close the window because there could still be data after the idling situation ends
				// so in this case, we publish an idle watermark
				for _, toBuffer := range d.toBuffers {
					if publisher, ok := d.watermarkPublishers[toBuffer.GetName()]; ok {
						idlehandler.PublishIdleWatermark(ctx, toBuffer, publisher, d.idleManager, d.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(watermark))
					}
				}
			}
		}
		return
	}
	readMessagesCount.With(map[string]string{
		metrics.LabelVertex:             d.vertexName,
		metrics.LabelPipeline:           d.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(d.vertexReplica)),
		"buffer":                        d.fromBuffer.GetName(),
	}).Add(float64(len(readMessages)))
	// fetch watermark using the first element's watermark, because we assign the watermark to all other
	// elements in the batch based on the watermark we fetch from 0th offset.
	processorWM := d.watermarkFetcher.GetWatermark(readMessages[0].ReadOffset)
	for _, m := range readMessages {
		if !d.keyed {
			m.Key = dfv1.DefaultKeyForNonKeyedData
			m.Message.Key = dfv1.DefaultKeyForNonKeyedData
		}
		m.Watermark = time.Time(processorWM)
		totalBytes += len(m.Payload)
	}
	readBytesCount.With(map[string]string{
		metrics.LabelVertex:             d.vertexName,
		metrics.LabelPipeline:           d.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(d.vertexReplica)),
		"buffer":                        d.fromBuffer.GetName(),
	}).Add(float64(totalBytes))

	// readMessages has to be written to PBQ, acked, etc.
	d.readloop.Process(ctx, readMessages)
}

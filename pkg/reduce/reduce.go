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
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/readloop"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/window"
)

// DataForward reads data from isb and forwards them to readloop
type DataForward struct {
	vertexInstance      *dfv1.VertexInstance
	fromBuffer          isb.BufferReader
	toBuffers           map[string]isb.BufferWriter
	readloop            *readloop.ReadLoop
	watermarkFetcher    fetch.Fetcher
	watermarkPublishers map[string]publish.Publisher
	windowingStrategy   window.Windower
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

	rl := readloop.NewReadLoop(ctx, udf, pbqManager, windowingStrategy, toBuffers, whereToDecider, watermarkPublishers)
	return &DataForward{
		vertexInstance:      vertexInstance,
		fromBuffer:          fromBuffer,
		toBuffers:           toBuffers,
		readloop:            rl,
		watermarkFetcher:    fw,
		watermarkPublishers: watermarkPublishers,
		windowingStrategy:   windowingStrategy,
		log:                 logging.FromContext(ctx),
		opts:                options}, nil
}

// Start starts forwarding messages to readloop
func (d *DataForward) Start(ctx context.Context) {
	err := d.readloop.Startup(ctx)
	if err != nil {
		d.log.Errorw("Failed to start the data forwarder in reduce vertex", zap.Error(err))
	}
	rCtx, rCancel := context.WithCancel(context.Background())
	for {
		select {
		case <-ctx.Done():
			d.log.Infow("Stopping reduce data forwarder...")
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := d.fromBuffer.Close(); err != nil {
				d.log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
			} else {
				d.log.Infow("Closed buffer reader", zap.String("bufferFrom", d.fromBuffer.GetName()))
			}

			// cancel the readloop context so that all the messages are persisted and acked.
			rCancel()
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
			d.forwardAChunk(rCtx)
		}
	}
}

// forwardAChunk reads a chunk of messages from isb and assigns watermark to messages
// and forwards the messages to readloop
func (d *DataForward) forwardAChunk(ctx context.Context) {
	readMessages, err := d.fromBuffer.Read(ctx, d.opts.readBatchSize)
	if err != nil {
		d.log.Errorw("Failed to read from isb", zap.Error(err))
	}

	if len(readMessages) == 0 {
		return
	}

	// fetch watermark using the first element's watermark, because we assign the watermark to all other
	// elements in the batch based on the watermark we fetch from 0th offset.
	processorWM := d.watermarkFetcher.GetWatermark(readMessages[0].ReadOffset)
	for _, m := range readMessages {
		if !d.vertexInstance.Vertex.Spec.UDF.GroupBy.Keyed {
			m.Key = dfv1.DefaultKeyForNonKeyedData
			m.Message.Key = dfv1.DefaultKeyForNonKeyedData
		}
		m.Watermark = time.Time(processorWM)
	}

	d.readloop.Process(ctx, readMessages)
}

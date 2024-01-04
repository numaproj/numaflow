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

// Package pnf processes and then forwards messages belonging to a window. It reads the data from PBQ (which is populated
// by the `data forwarder`), calls the UDF reduce function, and then forwards to the next ISB. After a successful forward, it
// invokes `GC` to clean up the PBQ. Since pnf is a reducer, it mutates the watermark. The watermark after the pnf will
// be the end time of the window.
package pnf

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
)

// processAndForward reads requests from pbq, invokes reduceApplier using grpc, forwards the results to ISB, and then publishes
// the watermark for that partition.
type processAndForward struct {
	vertexName         string
	pipelineName       string
	vertexReplica      int32
	partitionId        *partition.ID
	UDF                applier.ReduceApplier
	pbqReader          pbq.Reader
	log                *zap.SugaredLogger
	toBuffers          map[string][]isb.BufferWriter
	whereToDecider     forwarder.ToWhichStepDecider
	wmPublishers       map[string]publish.Publisher
	idleManager        wmb.IdleManager
	pbqManager         *pbq.Manager
	windower           window.TimedWindower
	latestWriteOffsets map[string][][]isb.Offset
	done               chan struct{}
}

// newProcessAndForward will return a new processAndForward instance
func newProcessAndForward(ctx context.Context,
	vertexName string,
	pipelineName string,
	vr int32,
	partitionID *partition.ID,
	udf applier.ReduceApplier,
	pbqReader pbq.Reader,
	toBuffers map[string][]isb.BufferWriter,
	whereToDecider forwarder.ToWhichStepDecider,
	pw map[string]publish.Publisher,
	idleManager wmb.IdleManager,
	manager *pbq.Manager,
	windower window.TimedWindower) *processAndForward {

	// latestWriteOffsets tracks the latest write offsets for each ISB buffer.
	// which will be used for publishing the watermark when the window is closed.
	latestWriteOffsets := make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range toBuffers {
		latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}

	pf := &processAndForward{
		vertexName:         vertexName,
		pipelineName:       pipelineName,
		vertexReplica:      vr,
		partitionId:        partitionID,
		UDF:                udf,
		pbqReader:          pbqReader,
		toBuffers:          toBuffers,
		whereToDecider:     whereToDecider,
		wmPublishers:       pw,
		idleManager:        idleManager,
		pbqManager:         manager,
		windower:           windower,
		done:               make(chan struct{}),
		latestWriteOffsets: latestWriteOffsets,
		log:                logging.FromContext(ctx),
	}
	// start the processAndForward routine. This go-routine is collected by the Shutdown method which
	// listens on the done channel.
	go pf.invokeUDF(ctx)
	return pf
}

// invokeUDF reads requests from the supplied PBQ, invokes the UDF to get the response, and then forwards
// the writeMessages to the ISBs. It also publishes the watermark and invokes GC on PBQ. This method invokes UDF in a async
// manner, which means it doesn't wait for the output of all the keys to be available before forwarding.
// The watermark is only published at COB at key level for Unaligned and at Partition level for Aligned.
func (p *processAndForward) invokeUDF(ctx context.Context) {
	defer close(p.done)
	responseCh, errCh := p.UDF.ApplyReduce(ctx, p.partitionId, p.pbqReader.ReadCh())

	if p.windower.Type() == window.Aligned {
		p.forwardAlignedWindowResponses(ctx, responseCh, errCh)
	} else {
		p.forwardUnalignedWindowResponses(ctx, responseCh, errCh)
	}
}

// forwardUnalignedWindowResponses writes to the next ISB along with the WM for the responses from the UDF for the unaligned windows.
func (p *processAndForward) forwardUnalignedWindowResponses(ctx context.Context, responseCh <-chan *window.TimedWindowResponse, errCh <-chan error) {
	// this for loop never exits because we do not track at the partition level but will be tracked at window level.
	// since the key is involved, we cannot ever do a cob at partition level.
outerLoop:
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, ctx.Err()) {
				return
			}
			if err != nil {
				p.log.Panic("Got an error while invoking ApplyReduce", zap.Error(err), zap.Any("partitionID", p.partitionId))
			}
		case response, ok := <-responseCh:
			if !ok {
				break outerLoop
			}

			// publish WMs now that we have a COB for the key.
			if response.EOF {
				// since we track session window for every key, we need to delete the closed windows
				// when we have received the EOF response from the UDF.
				// FIXME(session): we need to compact the pbq for unAligned when we have received the EOF response from the UDF.
				// we should not use p.partitionId here, we should use the partition id from the response.
				// because for unaligned p.partitionId indicates the shared partition id for the key.
				p.publishWM(ctx, response.Window.Partition())

				// delete the closed windows which are tracked by the windower
				p.windower.DeleteClosedWindow(response)
				continue
			}

			p.forwardToBuffers(ctx, response)
		}
	}
}

// forwardAlignedWindowResponses writes to the next ISB along with the WM for the responses from the UDF for the aligned window.
func (p *processAndForward) forwardAlignedWindowResponses(ctx context.Context, responseCh <-chan *window.TimedWindowResponse, errCh <-chan error) {
	// for loop for aligned windows does exit since we create a partition for every unique (start, end) window tuple.
outerLoop:
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, ctx.Err()) {
				return
			}
			if err != nil {
				p.log.Panic("Got an error while invoking ApplyReduce", zap.Error(err), zap.Any("partitionID", p.partitionId))
			}
		case response, ok := <-responseCh:
			if !ok {
				break outerLoop
			}
			if response.EOF {
				continue
			}

			p.forwardToBuffers(ctx, response)
		}
	}

	// for aligned we don't track the windows for every key, we need to delete the window
	// once we have received all the responses from the UDF.
	p.publishWM(ctx, p.partitionId)
	// delete the closed windows which are tracked by the windower
	p.windower.DeleteClosedWindow(&window.TimedWindowResponse{Window: window.NewWindowFromPartition(p.partitionId)})

	// Since we have successfully processed all the messages for a window, we can now delete the persisted messages.
	err := p.pbqReader.GC()
	p.log.Infow("Finished GC", zap.Any("partitionID", p.partitionId))
	if err != nil {
		p.log.Errorw("Got an error while invoking GC", zap.Error(err), zap.Any("partitionID", p.partitionId))
		return
	}
}

// forwardToBuffers writes the messages to the ISBs concurrently for each partition.
func (p *processAndForward) forwardToBuffers(ctx context.Context, response *window.TimedWindowResponse) {
	messagesToStep := p.whereToStep([]*isb.WriteMessage{response.WriteMessage})
	// parallel writes to each ISB
	var wg sync.WaitGroup
	var mu sync.Mutex

	for key, values := range messagesToStep {
		for index, messages := range values {
			if len(messages) == 0 {
				continue
			}
			wg.Add(1)
			go func(toVertexName string, toVertexPartitionIdx int32, resultMessages []isb.Message) {
				defer wg.Done()
				offsets := p.writeToBuffer(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
				mu.Lock()
				// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
				p.latestWriteOffsets[toVertexName][toVertexPartitionIdx] = offsets
				mu.Unlock()
			}(key, int32(index), messages)
		}
	}

	// wait until all the writer go routines return
	wg.Wait()
}

// whereToStep assigns a message to the ISBs based on the Message.Keys.
func (p *processAndForward) whereToStep(writeMessages []*isb.WriteMessage) map[string][][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][][]isb.Message)

	var to []forwarder.VertexBuffer
	var err error
	for _, msg := range writeMessages {
		to, err = p.whereToDecider.WhereTo(msg.Keys, msg.Tags)
		if err != nil {
			metrics.PlatformError.With(map[string]string{
				metrics.LabelVertex:             p.vertexName,
				metrics.LabelPipeline:           p.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
			}).Inc()
			p.log.Errorw("Got an error while invoking WhereTo, dropping the message", zap.Strings("keys", msg.Keys), zap.Error(err), zap.Any("partitionID", p.partitionId))
			continue
		}

		if len(to) == 0 {
			continue
		}

		for _, step := range to {
			if _, ok := messagesToStep[step.ToVertexName]; !ok {
				messagesToStep[step.ToVertexName] = make([][]isb.Message, len(p.toBuffers[step.ToVertexName]))
			}
			messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx] = append(messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx], msg.Message)
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
func (p *processAndForward) writeToBuffer(ctx context.Context, edgeName string, partition int32, resultMessages []isb.Message) []isb.Offset {
	var (
		writeCount int
		writeBytes float64
		dropBytes  float64
	)

	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	writeMessages := resultMessages

	// write to isb with infinite exponential backoff (until shutdown is triggered)
	var offsets []isb.Offset
	ctxClosedErr := wait.ExponentialBackoffWithContext(ctx, ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		var failedMessages []isb.Message
		offsets, writeErrs = p.toBuffers[edgeName][partition].Write(ctx, writeMessages)
		for i, message := range writeMessages {
			if writeErrs[i] != nil {
				if errors.As(writeErrs[i], &isb.NoRetryableBufferWriteErr{}) {
					// If toBuffer returns us a NoRetryableBufferWriteErr, we drop the message.
					dropBytes += float64(len(message.Payload))
				} else {
					failedMessages = append(failedMessages, message)
				}
			} else {
				writeCount++
				writeBytes += float64(len(message.Payload))
			}
		}
		// retry only the failed messages
		if len(failedMessages) > 0 {
			p.log.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			metrics.WriteMessagesError.With(map[string]string{
				metrics.LabelVertex:             p.vertexName,
				metrics.LabelPipeline:           p.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
				metrics.LabelPartitionName:      p.toBuffers[edgeName][partition].GetName()}).Add(float64(len(failedMessages)))
			return false, nil
		}
		return true, nil
	})

	if ctxClosedErr != nil {
		p.log.Errorw("Ctx closed while writing messages to ISB", zap.Error(ctxClosedErr), zap.Any("partitionID", p.partitionId))
		return nil
	}

	metrics.DropMessagesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		metrics.LabelPartitionName:      p.toBuffers[edgeName][partition].GetName()}).Add(float64(len(resultMessages) - writeCount))

	metrics.DropBytesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		metrics.LabelPartitionName:      p.toBuffers[edgeName][partition].GetName()}).Add(dropBytes)

	metrics.WriteMessagesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		metrics.LabelPartitionName:      p.toBuffers[edgeName][partition].GetName()}).Add(float64(writeCount))

	metrics.WriteBytesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		metrics.LabelPartitionName:      p.toBuffers[edgeName][partition].GetName()}).Add(writeBytes)
	return offsets
}

// publishWM publishes the watermark to each edge.
// TODO: support multi partitioned edges.
func (p *processAndForward) publishWM(ctx context.Context, id *partition.ID) {
	// publish watermark, we publish window end time minus one millisecond  as watermark
	// but if there's a window that's about to be closed which has a end time before the current window end time,
	// we publish that window's end time as watermark. This is to ensure that the watermark is monotonically increasing.
	var wm wmb.Watermark
	oldestClosedWindowEndTime := p.windower.OldestWindowEndTime()
	if oldestClosedWindowEndTime != time.UnixMilli(-1) && oldestClosedWindowEndTime.Before(p.partitionId.End) {
		wm = wmb.Watermark(oldestClosedWindowEndTime.Add(-1 * time.Millisecond))
	} else {
		wm = wmb.Watermark(id.End.Add(-1 * time.Millisecond))
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// Created as a slice since it tracks per partition of the buffer.
	var activeWatermarkBuffers = make(map[string][]bool)
	for toVertexName, bufferOffsets := range p.latestWriteOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(bufferOffsets))
		if publisher, ok := p.wmPublishers[toVertexName]; ok {
			for index, offsets := range bufferOffsets {
				if len(offsets) > 0 {
					publisher.PublishWatermark(wm, offsets[len(offsets)-1], int32(index))
					activeWatermarkBuffers[toVertexName][index] = true
					// reset because the toBuffer partition is not idling
					p.idleManager.Reset(p.toBuffers[toVertexName][index].GetName())
				}
			}
		}

	}

	// if there's any buffers that haven't received any watermark during this
	// batch processing cycle, send an idle watermark
	for toVertexName := range p.wmPublishers {
		for index, activePartition := range activeWatermarkBuffers[toVertexName] {
			if !activePartition {
				if publisher, ok := p.wmPublishers[toVertexName]; ok {
					idlehandler.PublishIdleWatermark(ctx, p.toBuffers[toVertexName][index], publisher, p.idleManager, p.log, dfv1.VertexTypeReduceUDF, wm)
				}
			}
		}
	}

	// reset the latestWriteOffsets after publishing watermark
	p.latestWriteOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range p.toBuffers {
		p.latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}
}

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

package pnf

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

// ProcessAndForward invokes the UDF for each partition separately in a go routine and writes the response to the ISBs.
// It also publishes the watermark and invokes GC on PBQ.
type ProcessAndForward struct {
	vertexName          string
	pipelineName        string
	vertexReplica       int32
	pbqManager          *pbq.Manager
	reduceApplier       applier.ReduceApplier
	toBuffers           map[string][]isb.BufferWriter
	whereToDecider      forwarder.ToWhichStepDecider
	watermarkPublishers map[string]publish.Publisher
	idleManager         wmb.IdleManager
	windower            window.TimedWindower
	pnfRoutines         map[string]chan struct{}
	responseCh          chan *window.TimedWindowResponse
	latestWriteOffsets  map[string][][]isb.Offset
	writeMessages       []*isb.WriteMessage
	opts                *options
	log                 *zap.SugaredLogger
	forwardDoneCh       chan struct{}
	mu                  sync.RWMutex
	sync.RWMutex
}

// NewProcessAndForward returns a new ProcessAndForward.
func NewProcessAndForward(ctx context.Context,
	vertexInstance *dfv1.VertexInstance,
	udf applier.ReduceApplier,
	toBuffers map[string][]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forwarder.ToWhichStepDecider,
	watermarkPublishers map[string]publish.Publisher,
	idleManager wmb.IdleManager,
	windower window.TimedWindower,
	opts ...Option) *ProcessAndForward {

	// apply the options
	dOpts := &options{
		batchSize:     dfv1.DefaultPnfBatchSize,
		flushDuration: dfv1.DefaultPnfFlushDuration,
	}
	for _, opt := range opts {
		if err := opt(dOpts); err != nil {
			logging.FromContext(ctx).Panic("Got an error while applying options", zap.Error(err))
		}
	}

	// latestWriteOffsets tracks the latest write offsets for each ISB buffer.
	// which will be used for publishing the watermark when the window is closed.
	latestWriteOffsets := make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range toBuffers {
		latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}

	pfManager := &ProcessAndForward{
		vertexName:          vertexInstance.Vertex.Spec.Name,
		pipelineName:        vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:       vertexInstance.Replica,
		pbqManager:          pbqManager,
		reduceApplier:       udf,
		toBuffers:           toBuffers,
		whereToDecider:      whereToDecider,
		watermarkPublishers: watermarkPublishers,
		idleManager:         idleManager,
		windower:            windower,
		responseCh:          make(chan *window.TimedWindowResponse),
		latestWriteOffsets:  latestWriteOffsets,
		writeMessages:       make([]*isb.WriteMessage, 0),
		pnfRoutines:         make(map[string]chan struct{}),
		log:                 logging.FromContext(ctx),
		forwardDoneCh:       make(chan struct{}),
		opts:                dOpts,
	}

	go pfManager.forwardResponses(ctx)

	return pfManager
}

// AsyncSchedulePnF creates a go routine for each partition to invoke the UDF.
// does not maintain the order of execution between partitions.
func (pm *ProcessAndForward) AsyncSchedulePnF(ctx context.Context,
	partitionID *partition.ID,
	pbq pbq.Reader,
) {
	doneCh := make(chan struct{})
	pm.mu.Lock()
	pm.pnfRoutines[partitionID.String()] = doneCh
	pm.mu.Unlock()
	go pm.invokeUDF(ctx, doneCh, partitionID, pbq)
}

// invokeUDF reads requests from the supplied PBQ, invokes the UDF to gets the response and writes the response to the
// main channel.
func (pm *ProcessAndForward) invokeUDF(ctx context.Context, done chan struct{}, pid *partition.ID, pbqReader pbq.Reader) {
	defer func() {
		pm.mu.Lock()
		delete(pm.pnfRoutines, pid.String())
		pm.mu.Unlock()
	}()

	defer close(done)
	udfResponseCh, errCh := pm.reduceApplier.ApplyReduce(ctx, pid, pbqReader.ReadCh())

outerLoop:
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				pm.log.Infow("Context is canceled, stopping the processAndForward", zap.Error(err))
				return
			}
			if err != nil {
				pm.log.Panic("Got an error while invoking ApplyReduce", zap.Error(err))
			}
		case response, ok := <-udfResponseCh:
			if !ok {
				break outerLoop
			}

			pm.responseCh <- response
		}
	}
}

// forwardResponses forwards the writeMessages to the ISBs. It also publishes the watermark and invokes GC on PBQ.
// The watermark is only published at COB at key level for Unaligned and at Partition level for Aligned.
func (pm *ProcessAndForward) forwardResponses(ctx context.Context) {
	defer close(pm.forwardDoneCh)

	flushTimer := time.NewTicker(pm.opts.flushDuration)

	for {
		select {
		case response, ok := <-pm.responseCh:
			if !ok {
				return
			}

			// Process the response
			err := pm.processResponse(ctx, response)
			// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
			// unless ctx.Done() happens)
			if err != nil {
				pm.log.Errorw("Error while processing response, ctx was canceled", zap.Error(err))
				return
			}

		case <-flushTimer.C:
			// if there are no messages to write, continue
			if len(pm.writeMessages) == 0 {
				continue
			}
			// Since flushTimer is triggered, forward the messages to the ISBs
			err := pm.forwardToBuffers(ctx)
			// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
			// unless ctx.Done() happens)
			if err != nil {
				pm.log.Errorw("Error while forwarding messages, ctx was canceled", zap.Error(err))
				return
			}
		}
	}
}

// processResponse processes a response received from the response channel.
func (pm *ProcessAndForward) processResponse(ctx context.Context, response *window.TimedWindowResponse) error {
	if response.EOF {
		err := pm.forwardToBuffers(ctx)
		// error will not be nil only when the context is closed, so we can safely return
		if err != nil {
			return err
		}

		// publish watermark
		pm.publishWM(ctx, response.Window.Partition())

		// delete the closed windows which are tracked by the windower
		pm.windower.DeleteClosedWindow(response.Window)

		// persist the GC event for unaligned window type (compactor will compact it) and invoke GC for aligned window type
		if pm.windower.Type() == window.Unaligned {
			err = pm.opts.gcEventsTracker.PersistGCEvent(response.Window)
			if err != nil {
				pm.log.Errorw("Got an error while tracking GC event", zap.Error(err), zap.String("windowID", response.Window.ID()))
			}
		} else {
			pid := *response.Window.Partition()

			if pbqReader := pm.pbqManager.GetPBQ(pid); pbqReader != nil {
				// Since we have successfully processed all the messages for a window, we can now delete the persisted messages.
				err = pbqReader.GC()
				if err != nil {
					pm.log.Errorw("Got an error while invoking GC", zap.Error(err), zap.String("partitionID", pid.String()))
					return err
				}
				pm.log.Infow("Finished GC", zap.String("partitionID", pid.String()))

			}

		}
		// we do not have to write anything as this is a EOF message
		return nil
	}

	pm.writeMessages = append(pm.writeMessages, response.WriteMessage)

	// if the batch size is reached, forward the messages to the ISBs
	if len(pm.writeMessages) > pm.opts.batchSize {
		err := pm.forwardToBuffers(ctx)
		// error will not be nil only when the context is closed, so we can safely return
		if err != nil {
			return err
		}
	}

	return nil
}

// forwardToBuffers writes the messages to the ISBs concurrently for each partition.
func (pm *ProcessAndForward) forwardToBuffers(ctx context.Context) error {
	messagesToStep := pm.whereToStep(pm.writeMessages)
	// parallel writes to each ISB
	var mu sync.Mutex
	// use error group
	var eg errgroup.Group
	for key, values := range messagesToStep {
		for index, messages := range values {
			if len(messages) == 0 {
				continue
			}

			func(toVertexName string, toVertexPartitionIdx int32, resultMessages []isb.Message) {
				eg.Go(func() error {
					offsets, err := pm.writeToBuffer(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
					if err != nil {
						return err
					}
					mu.Lock()
					// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
					pm.latestWriteOffsets[toVertexName][toVertexPartitionIdx] = offsets
					mu.Unlock()
					return nil
				})
			}(key, int32(index), messages)
		}
	}

	// wait until all the writer go routines return
	if err := eg.Wait(); err != nil {
		return err
	}

	pm.writeMessages = make([]*isb.WriteMessage, 0)
	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Keys.
func (pm *ProcessAndForward) whereToStep(writeMessages []*isb.WriteMessage) map[string][][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][][]isb.Message)

	var to []forwarder.VertexBuffer
	var err error
	for _, msg := range writeMessages {
		to, err = pm.whereToDecider.WhereTo(msg.Keys, msg.Tags)
		if err != nil {
			metrics.PlatformError.With(map[string]string{
				metrics.LabelVertex:             pm.vertexName,
				metrics.LabelPipeline:           pm.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
			}).Inc()
			pm.log.Errorw("Got an error while invoking WhereTo, dropping the message", zap.Strings("keys", msg.Keys), zap.Error(err))
			continue
		}

		if len(to) == 0 {
			continue
		}

		for _, step := range to {
			if _, ok := messagesToStep[step.ToVertexName]; !ok {
				messagesToStep[step.ToVertexName] = make([][]isb.Message, len(pm.toBuffers[step.ToVertexName]))
			}
			messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx] = append(messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx], msg.Message)
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
func (pm *ProcessAndForward) writeToBuffer(ctx context.Context, edgeName string, partition int32, resultMessages []isb.Message) ([]isb.Offset, error) {
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
	ctxClosedErr := wait.ExponentialBackoff(ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		var failedMessages []isb.Message
		offsets, writeErrs = pm.toBuffers[edgeName][partition].Write(ctx, writeMessages)
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
			pm.log.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			metrics.WriteMessagesError.With(map[string]string{
				metrics.LabelVertex:             pm.vertexName,
				metrics.LabelPipeline:           pm.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
				metrics.LabelPartitionName:      pm.toBuffers[edgeName][partition].GetName()}).Add(float64(len(failedMessages)))

			if ctx.Err() != nil {
				// no need to retry if the context is closed
				return false, ctx.Err()
			}
			// keep retrying...
			return false, nil
		}
		return true, nil
	})

	if ctxClosedErr != nil {
		pm.log.Errorw("Ctx closed while writing messages to ISB", zap.Error(ctxClosedErr))
		return nil, ctxClosedErr
	}

	metrics.DropMessagesCount.With(map[string]string{
		metrics.LabelVertex:             pm.vertexName,
		metrics.LabelPipeline:           pm.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
		metrics.LabelPartitionName:      pm.toBuffers[edgeName][partition].GetName()}).Add(float64(len(resultMessages) - writeCount))

	metrics.DropBytesCount.With(map[string]string{
		metrics.LabelVertex:             pm.vertexName,
		metrics.LabelPipeline:           pm.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
		metrics.LabelPartitionName:      pm.toBuffers[edgeName][partition].GetName()}).Add(dropBytes)

	metrics.WriteMessagesCount.With(map[string]string{
		metrics.LabelVertex:             pm.vertexName,
		metrics.LabelPipeline:           pm.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
		metrics.LabelPartitionName:      pm.toBuffers[edgeName][partition].GetName()}).Add(float64(writeCount))

	metrics.WriteBytesCount.With(map[string]string{
		metrics.LabelVertex:             pm.vertexName,
		metrics.LabelPipeline:           pm.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pm.vertexReplica)),
		metrics.LabelPartitionName:      pm.toBuffers[edgeName][partition].GetName()}).Add(writeBytes)
	return offsets, nil
}

// publishWM publishes the watermark to each edge.
func (pm *ProcessAndForward) publishWM(ctx context.Context, pid *partition.ID) {
	// publish watermark, we publish window end time minus one millisecond  as watermark
	// but if there's a window that's about to be closed which has a end time before the current window end time,
	// we publish that window's end time as watermark. This is to ensure that the watermark is monotonically increasing.
	wm := wmb.Watermark(time.UnixMilli(-1))
	if oldestClosedWindowEndTime := pm.windower.OldestWindowEndTime(); oldestClosedWindowEndTime.UnixMilli() != -1 {
		wm = wmb.Watermark(oldestClosedWindowEndTime.Add(-1 * time.Millisecond))
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// Created as a slice since it tracks per partition of the buffer.
	var activeWatermarkBuffers = make(map[string][]bool)
	for toVertexName, bufferOffsets := range pm.latestWriteOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(bufferOffsets))
		if publisher, ok := pm.watermarkPublishers[toVertexName]; ok {
			for index, offsets := range bufferOffsets {
				if len(offsets) > 0 {
					publisher.PublishWatermark(wm, offsets[len(offsets)-1], int32(index))
					activeWatermarkBuffers[toVertexName][index] = true
					// reset because the toBuffer partition is not idling
					pm.idleManager.MarkActive(wmb.PARTITION_0, pm.toBuffers[toVertexName][index].GetName())
				}
			}
		}

	}

	// if there's any buffers that haven't received any watermark during this
	// batch processing cycle, send an idle watermark
	for toVertexName := range pm.watermarkPublishers {
		for index, activePartition := range activeWatermarkBuffers[toVertexName] {
			if !activePartition {
				if publisher, ok := pm.watermarkPublishers[toVertexName]; ok {
					idlehandler.PublishIdleWatermark(ctx, wmb.PARTITION_0, pm.toBuffers[toVertexName][index], publisher, pm.idleManager, pm.log, pm.vertexName, pm.pipelineName, dfv1.VertexTypeReduceUDF, pm.vertexReplica, wm)
				}
			}
		}
	}

	// reset the latestWriteOffsets after publishing watermark
	pm.latestWriteOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range pm.toBuffers {
		pm.latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}
}

// Shutdown closes all the partitions of the buffer.
func (pm *ProcessAndForward) Shutdown() {
	pm.log.Infow("Shutting down ProcessAndForward")

	pm.mu.RLock()
	doneChs := make([]chan struct{}, 0, len(pm.pnfRoutines))
	for _, doneCh := range pm.pnfRoutines {
		doneChs = append(doneChs, doneCh)
	}
	pm.mu.RUnlock()

	for _, doneCh := range doneChs {
		<-doneCh
	}

	pm.log.Infow("All PnFs have finished, waiting for forwardDoneCh to be done")

	// close the response channel
	close(pm.responseCh)

	// wait for the forwardResponses to finish
	<-pm.forwardDoneCh

	// close all the buffers since the forwardResponses is done
	for _, buffer := range pm.toBuffers {
		for _, p := range buffer {
			if err := p.Close(); err != nil {
				pm.log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", p.GetName()))
			} else {
				pm.log.Infow("Closed partition writer", zap.String("bufferTo", p.GetName()))
			}
		}
	}

	pm.log.Infow("Successfully shutdown ProcessAndForward")
}

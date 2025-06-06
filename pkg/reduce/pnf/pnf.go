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
	"strings"
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
	opts                *options
	log                 *zap.SugaredLogger
	forwardDoneCh       chan struct{}
	mu                  sync.RWMutex
	lastSeenWindow      map[string]window.TimedWindow
	shutdownCh          chan struct{}
	shutdownOnce        sync.Once
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
	shutdownCh chan struct{},
	opts ...Option) *ProcessAndForward {

	// apply the options
	dOpts := &options{
		batchSize:     dfv1.DefaultReadBatchSize,
		flushDuration: dfv1.DefaultReadTimeout,
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
		pnfRoutines:         make(map[string]chan struct{}),
		log:                 logging.FromContext(ctx),
		forwardDoneCh:       make(chan struct{}),
		lastSeenWindow:      make(map[string]window.TimedWindow),
		shutdownCh:          shutdownCh,
		shutdownOnce:        sync.Once{},
		opts:                dOpts,
	}

	go pfManager.forwardResponses(ctx)

	return pfManager
}

// AsyncSchedulePnF creates a go routine for each partition to invoke the UDF.
// does not maintain the order of execution between partitions.
func (pf *ProcessAndForward) AsyncSchedulePnF(ctx context.Context, partitionID *partition.ID, pbq pbq.Reader) {
	doneCh := make(chan struct{})

	pf.mu.Lock()
	pf.pnfRoutines[partitionID.String()] = doneCh
	pf.mu.Unlock()

	go pf.invokeUDF(ctx, doneCh, partitionID, pbq)
}

// invokeUDF reads requests from the supplied PBQ, invokes the UDF to gets the response and writes the response to the
// main channel.
func (pf *ProcessAndForward) invokeUDF(ctx context.Context, done chan struct{}, pid *partition.ID, pbqReader pbq.Reader) {
	metricLabels := map[string]string{
		metrics.LabelVertex:             pf.vertexName,
		metrics.LabelPipeline:           pf.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pf.vertexReplica)),
	}
	start := time.Now()

	defer func() {
		pf.mu.Lock()
		delete(pf.pnfRoutines, pid.String())
		pf.mu.Unlock()
	}()

	defer close(done)
	udfResponseCh, errCh := pf.reduceApplier.ApplyReduce(ctx, pid, pbqReader.ReadCh())

outerLoop:
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				pf.log.Infow("Context is canceled, stopping the processAndForward", zap.Error(err))
				return
			}
			if err != nil {
				pf.log.Errorw("Got an error while invoking ApplyReduce", zap.Error(err))
				pf.shutdownOnce.Do(func() {
					close(pf.shutdownCh)
				})
				return
			}
		case response, ok := <-udfResponseCh:
			if !ok {
				break outerLoop
			}

			pf.responseCh <- response
		}
	}

	metrics.ReduceProcessTime.With(metricLabels).Observe(float64(time.Since(start).Microseconds()))
}

// forwardResponses forwards the writeMessages to the ISBs. It also publishes the watermark and invokes GC on PBQ.
// The watermark is only published at COB at key level for Unaligned and at Partition level for Aligned.
func (pf *ProcessAndForward) forwardResponses(ctx context.Context) {
	defer close(pf.forwardDoneCh)

	flushTimer := time.NewTicker(pf.opts.flushDuration)
	writeMessages := make([]*isb.WriteMessage, 0, pf.opts.batchSize)

	// should we flush?
	var flush bool

	// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
	// unless ctx.Done() happens)
forwardLoop:
	for {
		select {
		case response, ok := <-pf.responseCh:
			if !ok {
				break forwardLoop
			}

			if response.EOF {
				if err := pf.forwardToBuffers(ctx, &writeMessages); err != nil {
					return
				}

				if err := pf.handleEOFWindow(ctx, response.Window); err != nil {
					return
				}

				// delete the entry for the key from the lastSeenWindow map
				delete(pf.lastSeenWindow, strings.Join(response.Window.Keys(), dfv1.KeysDelimitter))

				// we do not have to write anything as this is an EOF message
				continue
			}

			// append the write message to the array
			writeMessages = append(writeMessages, response.WriteMessage)

			// if the batch size is reached, let's flush
			if len(writeMessages) >= pf.opts.batchSize {
				flush = true
			}

			if pf.windower.Strategy() == window.Accumulator {
				winKey := strings.Join(response.Window.Keys(), dfv1.KeysDelimitter)
				if win, ok := pf.lastSeenWindow[winKey]; ok {
					// to avoid writing a gc event for the same window end time multiple times, we only write when the
					// window end time is before the last seen window end time.
					if win.EndTime().Before(response.Window.EndTime()) {
						if err := pf.handleEOFWindow(ctx, response.Window); err != nil {
							return
						}
						pf.lastSeenWindow[winKey] = response.Window
					}
				} else {
					pf.lastSeenWindow[winKey] = response.Window
				}
			}

		case <-flushTimer.C:
			// if there are no messages to write, continue
			if len(writeMessages) == 0 {
				continue
			}

			// Since flushTimer is triggered, it is time to flush
			flush = true
		}

		if flush {
			if err := pf.forwardToBuffers(ctx, &writeMessages); err != nil {
				return
			}
			flush = false
		}
	}

	// if there are any messages left, forward them to the ISB
	if len(writeMessages) > 0 {
		if err := pf.forwardToBuffers(ctx, &writeMessages); err != nil {
			return
		}
	}
}

// handleEOFWindow handles the EOF response received from the response channel. It publishes the watermark and invokes GC on PBQ.
func (pf *ProcessAndForward) handleEOFWindow(ctx context.Context, eofWindow window.TimedWindow) error {
	// publish watermark
	pf.publishWM(ctx)

	// delete the closed windows which are tracked by the windower
	pf.windower.DeleteClosedWindow(eofWindow)

	var infiniteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	// persist the GC event for unaligned window type (compactor will compact it) and invoke GC for aligned window type.
	if pf.windower.Type() == window.Unaligned {
		err := wait.ExponentialBackoff(infiniteBackoff, func() (done bool, err error) {
			var attempt int
			err = pf.opts.gcEventsTracker.PersistGCEvent(eofWindow)
			if err != nil {
				attempt++
				pf.log.Errorw("Got an error while tracking GC event", zap.Error(err), zap.String("windowID", eofWindow.ID()), zap.Int("attempt", attempt))
				// no point retrying if ctx.Done has been invoked
				select {
				case <-ctx.Done():
					// no point in retrying after we have been asked to stop.
					return false, ctx.Err()
				default:
					// keep retrying
					return false, nil
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	} else {
		pid := *eofWindow.Partition()
		pbqReader := pf.pbqManager.GetPBQ(*eofWindow.Partition())
		err := wait.ExponentialBackoff(infiniteBackoff, func() (done bool, err error) {
			var attempt int
			err = pbqReader.GC()
			if err != nil {
				attempt++
				pf.log.Errorw("Got an error while invoking GC on PBQ", zap.Error(err), zap.String("partitionID", pid.String()), zap.Int("attempt", attempt))
				// no point retrying if ctx.Done has been invoked
				select {
				case <-ctx.Done():
					// no point in retrying after we have been asked to stop.
					return false, ctx.Err()
				default:
					// keep retrying
					return false, nil
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		pf.log.Infow("Partition Closed", zap.String("partitionID", pid.String()))
	}
	return nil
}

// forwardToBuffers writes the messages to the ISBs concurrently for each partition.
func (pf *ProcessAndForward) forwardToBuffers(ctx context.Context, writeMessages *[]*isb.WriteMessage) error {
	if len(*writeMessages) == 0 {
		return nil
	}

	messagesToStep := pf.whereToStep(*writeMessages)
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
					offsets, err := pf.writeToBuffer(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
					if err != nil {
						return err
					}
					mu.Lock()
					// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
					pf.latestWriteOffsets[toVertexName][toVertexPartitionIdx] = offsets
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

	// clear the writeMessages
	*writeMessages = make([]*isb.WriteMessage, 0, pf.opts.batchSize)
	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Keys.
func (pf *ProcessAndForward) whereToStep(writeMessages []*isb.WriteMessage) map[string][][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][][]isb.Message)

	var to []forwarder.VertexBuffer
	var err error
	for _, msg := range writeMessages {
		to, err = pf.whereToDecider.WhereTo(msg.Keys, msg.Tags, msg.ID.String())
		if err != nil {
			metrics.PlatformError.With(map[string]string{
				metrics.LabelVertex:             pf.vertexName,
				metrics.LabelPipeline:           pf.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pf.vertexReplica)),
			}).Inc()
			pf.log.Errorw("Got an error while invoking WhereTo, dropping the message", zap.Strings("keys", msg.Keys), zap.Error(err))
			continue
		}

		if len(to) == 0 {
			continue
		}

		for _, step := range to {
			if _, ok := messagesToStep[step.ToVertexName]; !ok {
				messagesToStep[step.ToVertexName] = make([][]isb.Message, len(pf.toBuffers[step.ToVertexName]))
			}
			messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx] = append(messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx], msg.Message)
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
func (pf *ProcessAndForward) writeToBuffer(ctx context.Context, edgeName string, partition int32, resultMessages []isb.Message) ([]isb.Offset, error) {
	var (
		writeCount int
		writeBytes float64
	)

	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	// initialize metric label
	metricLabelsWithPartition := map[string]string{
		metrics.LabelVertex:             pf.vertexName,
		metrics.LabelPipeline:           pf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pf.vertexReplica)),
		metrics.LabelPartitionName:      pf.toBuffers[edgeName][partition].GetName(),
	}

	writeStart := time.Now()
	writeMessages := resultMessages

	// write to isb with infinite exponential backoff (until shutdown is triggered)
	var offsets []isb.Offset
	ctxClosedErr := wait.ExponentialBackoff(ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		var failedMessages []isb.Message
		offsets, writeErrs = pf.toBuffers[edgeName][partition].Write(ctx, writeMessages)
		for i, message := range writeMessages {
			writeErr := writeErrs[i]
			if writeErr != nil {
				// Non retryable error, drop the message. Non retryable errors are only returned
				// when the buffer is full and the user has set the buffer full strategy to
				// DiscardLatest or when the message is duplicate.
				if errors.As(writeErr, &isb.NonRetryableBufferWriteErr{}) {
					metricLabelWithReason := map[string]string{
						metrics.LabelVertex:             pf.vertexName,
						metrics.LabelPipeline:           pf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(pf.vertexReplica)),
						metrics.LabelPartitionName:      pf.toBuffers[edgeName][partition].GetName(),
						metrics.LabelReason:             writeErr.Error(),
					}
					metrics.DropMessagesCount.With(metricLabelWithReason).Inc()
					metrics.DropBytesCount.With(metricLabelWithReason).Add(float64(len(message.Payload)))
					pf.log.Infow("Dropped message", zap.String("reason", writeErr.Error()), zap.String("vertex", pf.vertexName), zap.String("pipeline", pf.pipelineName), zap.String("msg_id", message.ID.String()))
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
			pf.log.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			metrics.WriteMessagesError.With(metricLabelsWithPartition).Add(float64(len(failedMessages)))

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
		pf.log.Errorw("Ctx closed while writing messages to ISB", zap.Error(ctxClosedErr))
		return nil, ctxClosedErr
	}

	metrics.WriteProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(writeStart).Microseconds()))
	metrics.WriteMessagesCount.With(metricLabelsWithPartition).Add(float64(writeCount))
	metrics.WriteBytesCount.With(metricLabelsWithPartition).Add(writeBytes)
	return offsets, nil
}

// publishWM publishes the watermark to each edge.
func (pf *ProcessAndForward) publishWM(ctx context.Context) {
	// publish watermark, we publish window end time minus one millisecond  as watermark
	// but if there's a window that's about to be closed which has a end time before the current window end time,
	// we publish that window's end time as watermark. This is to ensure that the watermark is monotonically increasing.
	wm := wmb.Watermark(time.UnixMilli(-1))
	if oldestClosedWindowEndTime := pf.windower.OldestWindowEndTime(); oldestClosedWindowEndTime.UnixMilli() != -1 {
		wm = wmb.Watermark(oldestClosedWindowEndTime.Add(-1 * time.Millisecond))
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// Created as a slice since it tracks per partition of the buffer.
	var activeWatermarkBuffers = make(map[string][]bool)
	for toVertexName, bufferOffsets := range pf.latestWriteOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(bufferOffsets))
		if publisher, ok := pf.watermarkPublishers[toVertexName]; ok {
			for index, offsets := range bufferOffsets {
				if len(offsets) > 0 && offsets[len(offsets)-1] != nil {
					publisher.PublishWatermark(wm, offsets[len(offsets)-1], int32(index))
					activeWatermarkBuffers[toVertexName][index] = true
					// reset because the toBuffer partition is not idling
					pf.idleManager.MarkActive(wmb.PARTITION_0, pf.toBuffers[toVertexName][index].GetName())
				}
			}
		}
	}

	// if there's any buffers that haven't received any watermark during this
	// batch processing cycle, send an idle watermark
	for toVertexName := range pf.watermarkPublishers {
		for index, activePartition := range activeWatermarkBuffers[toVertexName] {
			if !activePartition {
				if publisher, ok := pf.watermarkPublishers[toVertexName]; ok {
					idlehandler.PublishIdleWatermark(ctx, wmb.PARTITION_0, pf.toBuffers[toVertexName][index], publisher, pf.idleManager, pf.log, pf.vertexName, pf.pipelineName, dfv1.VertexTypeReduceUDF, pf.vertexReplica, wm)
				}
			}
		}
	}

	// reset the latestWriteOffsets after publishing watermark
	pf.latestWriteOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range pf.toBuffers {
		pf.latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}
}

// Shutdown closes all the partitions of the buffer.
func (pf *ProcessAndForward) Shutdown() {
	pf.log.Infow("Shutting down ProcessAndForward")

	pf.mu.RLock()
	doneChs := make([]chan struct{}, 0, len(pf.pnfRoutines))
	for _, doneCh := range pf.pnfRoutines {
		doneChs = append(doneChs, doneCh)
	}
	pf.mu.RUnlock()

	for _, doneCh := range doneChs {
		<-doneCh
	}
	pf.log.Infow("All PnFs have finished, waiting for forwardDoneCh to be done")

	// close the response channel
	close(pf.responseCh)

	// wait for the forwardResponses to finish
	<-pf.forwardDoneCh

	// close all the buffers since the forwardResponses is done
	for _, buffer := range pf.toBuffers {
		for _, p := range buffer {
			if err := p.Close(); err != nil {
				pf.log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", p.GetName()))
			} else {
				pf.log.Infow("Closed partition writer", zap.String("bufferTo", p.GetName()))
			}
		}
	}

	pf.log.Infow("Successfully shutdown ProcessAndForward")
}

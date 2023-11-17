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
	"github.com/numaproj/numaflow/pkg/forward"
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

// ProcessAndForward reads messages from pbq, invokes reduceApplier using grpc, forwards the results to ISB, and then publishes
// the watermark for that partition.
type ProcessAndForward struct {
	vertexName     string
	pipelineName   string
	vertexReplica  int32
	PartitionID    partition.ID
	UDF            applier.ReduceApplier
	windowResponse *window.TimedWindowResponse
	pbqReader      pbq.Reader
	log            *zap.SugaredLogger
	toBuffers      map[string][]isb.BufferWriter
	whereToDecider forward.ToWhichStepDecider
	wmPublishers   map[string]publish.Publisher
	idleManager    wmb.IdleManager
	pbqManager     *pbq.Manager
	windower       window.TimedWindower
}

// newProcessAndForward will return a new ProcessAndForward instance
func newProcessAndForward(ctx context.Context,
	vertexName string,
	pipelineName string,
	vr int32,
	partitionID partition.ID,
	udf applier.ReduceApplier,
	pbqReader pbq.Reader,
	toBuffers map[string][]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider,
	pw map[string]publish.Publisher,
	idleManager wmb.IdleManager,
	manager *pbq.Manager,
	windower window.TimedWindower) *ProcessAndForward {

	pf := &ProcessAndForward{
		vertexName:     vertexName,
		pipelineName:   pipelineName,
		vertexReplica:  vr,
		PartitionID:    partitionID,
		UDF:            udf,
		pbqReader:      pbqReader,
		log:            logging.FromContext(ctx),
		toBuffers:      toBuffers,
		whereToDecider: whereToDecider,
		wmPublishers:   pw,
		idleManager:    idleManager,
		pbqManager:     manager,
		windower:       windower,
	}

	return pf
}

// Hmm.. Global Window is nothing but a session window with infinite gap duration?

// et 60 75 69 86
// wm 59 69 69 86

// 60 - 70
// 75 - 85
// since we got 69 with watermark 69, we have to merge the two windows and the updated window
// should be 60 - 85
// now we got a message with et 86, and wm 86 we can materialize the window 60 - 85
// and publish the watermark as 85

// Process method reads messages from the supplied PBQ, invokes UDF to reduce the windowResponse.
func (p *ProcessAndForward) Process(ctx context.Context) error {
	var err error
	defer func(t time.Time) {
		metrics.ReduceProcessTime.With(map[string]string{
			metrics.LabelVertex:             p.vertexName,
			metrics.LabelPipeline:           p.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		}).Observe(float64(time.Since(t).Milliseconds()))
	}(time.Now())

	// blocking call, only returns the windowResponse after it has read all the requests from pbq
	p.windowResponse, err = p.UDF.ApplyReduce(ctx, &p.PartitionID, p.pbqReader.ReadCh())
	return err
}

// AsyncProcessForward reads messages from the supplied PBQ, invokes UDF to reduce the windowResponse, and then forwards
// the windowResponse to the ISBs. It also publishes the watermark and invokes GC on PBQ. This method invokes UDF in a async
// manner, which means it doesn't wait for the output of all the keys to be available before forwarding.
func (p *ProcessAndForward) AsyncProcessForward(ctx context.Context) {
	responseCh, errCh := p.UDF.AsyncApplyReduce(ctx, &p.PartitionID, p.pbqReader.ReadCh())

outerLoop:
	for {
		select {
		case err := <-errCh:
			if err == ctx.Err() {
				return
			}
			if err != nil {
				p.log.Panic("Got an error while invoking AsyncApplyReduce", zap.Error(err), zap.Any("partitionID", p.PartitionID))
			}
		case response, ok := <-responseCh:
			if !ok || response == nil {
				break outerLoop
			}
			messagesToStep := p.whereToStep(response.WriteMessages)
			// store write offsets to publish watermark
			writeOffsets := make(map[string][][]isb.Offset)
			for toVertexName, toVertexBuffer := range p.toBuffers {
				writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
			}

			// parallel writes to each isb
			var wg sync.WaitGroup
			var mu sync.Mutex
			for key, value := range messagesToStep {
				for index, messages := range value {
					if len(messages) == 0 {
						continue
					}
					wg.Add(1)
					go func(toVertexName string, toVertexPartitionIdx int32, resultMessages []isb.Message) {
						defer wg.Done()
						offsets := p.writeToBuffer(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
						mu.Lock()
						// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
						writeOffsets[toVertexName][toVertexPartitionIdx] = offsets
						mu.Unlock()
					}(key, int32(index), messages)
				}
			}

			// wait until all the writer go routines return
			wg.Wait()

			// publish watermark, we publish window end time as watermark
			// but if there's a window that's about to be closed which has a end time before the current window end time,
			// we publish that window's end time as watermark. This is to ensure that the watermark is monotonically increasing.
			oldestClosedWindowEndTime := p.windower.OldestClosedWindowEndTime()
			if oldestClosedWindowEndTime != time.UnixMilli(-1) && oldestClosedWindowEndTime.Before(response.ID.End) {
				p.publishWM(ctx, wmb.Watermark(oldestClosedWindowEndTime.Add(-1*time.Millisecond)), writeOffsets)
			} else {
				p.publishWM(ctx, wmb.Watermark(p.PartitionID.End.Add(-1*time.Millisecond)), writeOffsets)
			}

			// since we track window for every key, we need to delete the closed windows
			// every time we receive a response from the UDF.
			if p.windower.Strategy() == window.Session {
				// delete the closed windows which are tracked by the windower
				p.windower.DeleteClosedWindows(&window.TimedWindowResponse{ID: &p.PartitionID})
			}
		}
	}

	// since we don't track the windows for every key, we need to delete the closed windows
	// once we have received all the responses from the UDF.
	if p.windower.Strategy() == window.Fixed || p.windower.Strategy() == window.Sliding {
		// delete the closed windows which are tracked by the windower
		p.windower.DeleteClosedWindows(&window.TimedWindowResponse{ID: &p.PartitionID})
	}

	// Since we have successfully processed all the messages for a window, we can now delete the persisted messages.
	err := p.pbqReader.GC()
	p.log.Infow("Finished GC", zap.Any("partitionID", p.PartitionID))
	if err != nil {
		p.log.Errorw("Got an error while invoking GC", zap.Error(err), zap.Any("partitionID", p.PartitionID))
		return
	}
}

// Forward writes messages to the ISBs, publishes watermark, and invokes GC on PBQ.
func (p *ProcessAndForward) Forward(ctx context.Context) error {
	// extract window end time from the partitionID, which will be used for watermark
	defer func(t time.Time) {
		metrics.ReduceForwardTime.With(map[string]string{
			metrics.LabelVertex:             p.vertexName,
			metrics.LabelPipeline:           p.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		}).Observe(float64(time.Since(t).Microseconds()))
	}(time.Now())

	// millisecond is the lowest granularity currently supported.
	processorWM := wmb.Watermark(p.PartitionID.End.Add(-1 * time.Millisecond))

	messagesToStep := p.whereToStep(p.windowResponse.WriteMessages)

	// store write offsets to publish watermark
	writeOffsets := make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range p.toBuffers {
		writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}

	// parallel writes to each isb
	var wg sync.WaitGroup
	var mu sync.Mutex
	for key, value := range messagesToStep {
		for index, messages := range value {
			if len(messages) == 0 {
				continue
			}
			wg.Add(1)
			go func(toVertexName string, toVertexPartitionIdx int32, resultMessages []isb.Message) {
				defer wg.Done()
				offsets := p.writeToBuffer(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
				mu.Lock()
				// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
				writeOffsets[toVertexName][toVertexPartitionIdx] = offsets
				mu.Unlock()
			}(key, int32(index), messages)
		}
	}

	// wait until all the writer go routines return
	wg.Wait()

	p.publishWM(ctx, processorWM, writeOffsets)
	// delete the persisted messages
	err := p.pbqReader.GC()
	if err != nil {
		return err
	}
	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Keys.
func (p *ProcessAndForward) whereToStep(writeMessages []*isb.WriteMessage) map[string][][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][][]isb.Message)

	var to []forward.VertexBuffer
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
			p.log.Errorw("Got an error while invoking WhereTo, dropping the message", zap.Strings("keys", msg.Keys), zap.Error(err), zap.Any("partitionID", p.PartitionID))
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
// TODO: is there any point in returning an error here? this is an infinite loop and the only error is ctx.Done!
func (p *ProcessAndForward) writeToBuffer(ctx context.Context, edgeName string, partition int32, resultMessages []isb.Message) []isb.Offset {
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
		p.log.Errorw("Ctx closed while writing messages to ISB", zap.Error(ctxClosedErr), zap.Any("partitionID", p.PartitionID))
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
func (p *ProcessAndForward) publishWM(ctx context.Context, wm wmb.Watermark, writeOffsets map[string][][]isb.Offset) {
	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// Created as a slice since it tracks per partition of the buffer.
	var activeWatermarkBuffers = make(map[string][]bool)
	for toVertexName, bufferOffsets := range writeOffsets {
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
}

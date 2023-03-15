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
// by the `readloop`), calls the UDF reduce function, and then forwards to the next ISB. After a successful forward, it
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

	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// ProcessAndForward reads messages from pbq, invokes udf using grpc, forwards the results to ISB, and then publishes
// the watermark for that partition.
type ProcessAndForward struct {
	vertexName       string
	pipelineName     string
	vertexReplica    int32
	PartitionID      partition.ID
	UDF              applier.ReduceApplier
	result           []*isb.Message
	pbqReader        pbq.Reader
	log              *zap.SugaredLogger
	toBuffers        map[string]isb.BufferWriter
	whereToDecider   forward.ToWhichStepDecider
	publishWatermark map[string]publish.Publisher
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(ctx context.Context,
	vertexName string,
	pipelineName string,
	vr int32,
	partitionID partition.ID,
	udf applier.ReduceApplier,
	pbqReader pbq.Reader,
	toBuffers map[string]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider, pw map[string]publish.Publisher) *ProcessAndForward {
	return &ProcessAndForward{
		vertexName:       vertexName,
		pipelineName:     pipelineName,
		vertexReplica:    vr,
		PartitionID:      partitionID,
		UDF:              udf,
		pbqReader:        pbqReader,
		log:              logging.FromContext(ctx),
		toBuffers:        toBuffers,
		whereToDecider:   whereToDecider,
		publishWatermark: pw,
	}
}

// Process method reads messages from the supplied PBQ, invokes UDF to reduce the result.
func (p *ProcessAndForward) Process(ctx context.Context) error {
	var err error
	startTime := time.Now()
	defer reduceProcessTime.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
	}).Observe(float64(time.Since(startTime).Milliseconds()))

	// blocking call, only returns the result after it has read all the messages from pbq
	p.result, err = p.UDF.ApplyReduce(ctx, &p.PartitionID, p.pbqReader.ReadCh())
	return err
}

// Forward writes messages to the ISBs, publishes watermark, and invokes GC on PBQ.
func (p *ProcessAndForward) Forward(ctx context.Context) error {
	// extract window end time from the partitionID, which will be used for watermark
	startTime := time.Now()
	defer reduceForwardTime.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
	}).Observe(float64(time.Since(startTime).Microseconds()))

	// millisecond is the lowest granularity currently supported.
	processorWM := wmb.Watermark(p.PartitionID.End.Add(-1 * time.Millisecond))

	messagesToStep := p.whereToStep()

	// store write offsets to publish watermark
	writeOffsets := make(map[string][]isb.Offset)

	// parallel writes to each isb
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := true
	for key, messages := range messagesToStep {
		bufferID := key
		if len(messages) == 0 {
			continue
		}
		wg.Add(1)
		resultMessages := messages
		go func() {
			defer wg.Done()
			offsets, ctxClosedErr := p.writeToBuffer(ctx, bufferID, resultMessages)
			if ctxClosedErr != nil {
				success = false
				p.log.Errorw("Context closed while waiting to write the message to ISB", zap.Error(ctxClosedErr), zap.Any("partitionID", p.PartitionID))
				return
			}
			mu.Lock()
			// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
			writeOffsets[bufferID] = offsets
			mu.Unlock()
		}()
	}

	// wait until all the writer go routines return
	wg.Wait()
	// even if one write go routines fails, don't GC just return
	if !success {
		return errors.New("failed to forward the messages to isb")
	}

	p.publishWM(processorWM, writeOffsets)
	// delete the persisted messages
	err := p.pbqReader.GC()
	if err != nil {
		return err
	}
	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Key.
// TODO: we have to introduce support for shuffle, output of a reducer can be input to the next reducer.
func (p *ProcessAndForward) whereToStep() map[string][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][]isb.Message)

	var to []string
	var err error
	for _, msg := range p.result {
		to, err = p.whereToDecider.WhereTo(msg.Key)
		if err != nil {
			platformError.With(map[string]string{
				metrics.LabelVertex:             p.vertexName,
				metrics.LabelPipeline:           p.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
			}).Inc()
			p.log.Errorw("Got an error while invoking WhereTo, dropping the message", zap.String("key", msg.Key), zap.Error(err), zap.Any("partitionID", p.PartitionID))
			continue
		}

		if len(to) == 0 {
			continue
		}

		for _, bufferID := range to {
			if _, ok := messagesToStep[bufferID]; !ok {
				messagesToStep[bufferID] = make([]isb.Message, 0)
			}
			messagesToStep[bufferID] = append(messagesToStep[bufferID], *msg)
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
// TODO: is there any point in returning an error here? this is an infinite loop and the only error is ctx.Done!
func (p *ProcessAndForward) writeToBuffer(ctx context.Context, bufferID string, resultMessages []isb.Message) ([]isb.Offset, error) {
	var totalBytes float64
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
		offsets, writeErrs = p.toBuffers[bufferID].Write(ctx, writeMessages)
		for i, message := range writeMessages {
			if writeErrs[i] != nil {
				failedMessages = append(failedMessages, message)
			} else {
				totalBytes += float64(len(message.Payload))
				p.log.Debugw("Forwarded message", zap.String("bufferID", bufferID), zap.Any("message", message))
			}
		}
		// retry only the failed messages
		if len(failedMessages) > 0 {
			p.log.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			writeMessagesError.With(map[string]string{
				metrics.LabelVertex:             p.vertexName,
				metrics.LabelPipeline:           p.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
				"buffer":                        bufferID}).Add(float64(len(failedMessages)))
			return false, nil
		}
		return true, nil
	})

	writeMessagesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		"buffer":                        bufferID}).Add(float64(len(resultMessages)))

	writeBytesCount.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
		"buffer":                        bufferID}).Add(totalBytes)
	return offsets, ctxClosedErr
}

// publishWM publishes the watermark to each edge.
func (p *ProcessAndForward) publishWM(wm wmb.Watermark, writeOffsets map[string][]isb.Offset) {
	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	var activeWatermarkBuffers = make(map[string]bool)
	for bufferName, offsets := range writeOffsets {
		if publisher, ok := p.publishWatermark[bufferName]; ok {
			if len(offsets) > 0 {
				publisher.PublishWatermark(wm, offsets[len(offsets)-1])
				activeWatermarkBuffers[bufferName] = true
			}
		}
	}
	if len(activeWatermarkBuffers) < len(p.publishWatermark) {
		// if there's any buffers that haven't received any watermark during this
		// batch processing cycle, send an idle watermark
		for bufferName := range p.publishWatermark {
			if !activeWatermarkBuffers[bufferName] {
				// use the watermark of the current read batch for the idle watermark
				// we don't care about the offset here because, in this use case,
				// we will use a referred watermark to replace this idle watermark
				p.publishWatermark[bufferName].PublishIdleWatermark(wm, isb.SimpleIntOffset(func() int64 { return -1 }))
			}
		}
	}
}

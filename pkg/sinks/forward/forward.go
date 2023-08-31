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

package forward

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// DataForward forwards the data from previous vertex to sinker.
type DataForward struct {
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn            context.CancelFunc
	fromBufferPartition isb.BufferReader
	toBuffer            isb.BufferWriter
	wmFetcher           fetch.Fetcher
	wmPublisher         publish.Publisher
	opts                options
	vertexName          string
	pipelineName        string
	// idleManager manages the idle watermark status.
	idleManager *wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid.
	wmbChecker wmb.WMBChecker
	Shutdown
}

// NewDataForward creates a sink data forwarder.
func NewDataForward(
	vertex *dfv1.Vertex,
	fromStep isb.BufferReader,
	toSteps isb.BufferWriter,
	fetchWatermark fetch.Fetcher,
	publishWatermark publish.Publisher,
	opts ...Option) (*DataForward, error) {

	options := DefaultOptions()
	for _, o := range opts {
		if err := o(options); err != nil {
			return nil, err
		}
	}
	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	var df = DataForward{
		ctx:                 ctx,
		cancelFn:            cancel,
		fromBufferPartition: fromStep,
		toBuffer:            toSteps,
		wmFetcher:           fetchWatermark,
		wmPublisher:         publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:   vertex.Spec.Name,
		pipelineName: vertex.Spec.PipelineName,
		idleManager:  wmb.NewIdleManager(1),
		wmbChecker:   wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}

	// Add logger from parent ctx to child context.
	df.ctx = logging.WithLogger(ctx, options.logger)

	return &df, nil
}

// Start starts reading the buffer and forwards to sinker. Call `Stop` to stop.
func (df *DataForward) Start() <-chan struct{} {
	log := logging.FromContext(df.ctx)
	stopped := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Info("Starting sink forwarder...")
		// with wg approach can do more cleanup in case we need in the future.
		defer wg.Done()
		for {
			select {
			case <-df.ctx.Done():
				ok, err := df.IsShuttingDown()
				if err != nil {
					// ignore the error for now.
					log.Errorw("Failed to check if it can shutdown", zap.Error(err))
				}
				if ok {
					log.Info("Shutting down...")
					return
				}
			default:
				// once context.Done() is called, we still have to try to forwardAChunk because in graceful
				// shutdown the fromBufferPartition should be empty.
			}
			// keep doing what you are good at
			df.forwardAChunk(df.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// Clean up resources for buffer reader and all the writers if any.
		if err := df.fromBufferPartition.Close(); err != nil {
			log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed buffer reader", zap.String("bufferFrom", df.fromBufferPartition.GetName()))
		}
		toBuffer := df.toBuffer
		if err := toBuffer.Close(); err != nil {
			log.Errorw("Failed to close toBuffer writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", toBuffer.GetName()))
		} else {
			log.Infow("Closed toBuffer writer", zap.String("bufferTo", toBuffer.GetName()))
		}

		close(stopped)
	}()

	return stopped
}

// readWriteMessagePair represents a read message and its processed write messages.
type readWriteMessagePair struct {
	readMessage   *isb.ReadMessage
	writeMessages []*isb.WriteMessage
}

// forwardAChunk forwards a chunk of message from the fromBufferPartition to the toBuffer. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to WhereTo, etc.
func (df *DataForward) forwardAChunk(ctx context.Context) {
	start := time.Now()
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := df.fromBufferPartition.Read(ctx, df.opts.readBatchSize)
	df.opts.logger.Debugw("Read from buffer", zap.String("bufferFrom", df.fromBufferPartition.GetName()), zap.Int64("length", int64(len(readMessages))))
	if err != nil {
		df.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
		readMessagesError.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Inc()
	}
	readMessagesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Add(float64(len(readMessages)))

	// process only if we have any read messages. There is a natural looping here if there is an internal error while
	// reading, and we are not able to proceed.
	if len(readMessages) == 0 {
		// When the read length is zero, the write length is definitely zero too,
		// meaning there's no data to be published to the next vertex, and we consider this
		// situation as idling.
		// In order to continue propagating watermark, we will set watermark idle=true and publish it.
		// We also publish a control message if this is the first time we get this idle situation.
		// We compute the HeadIdleWMB using the given partition as the idle watermark
		var processorWMB = df.wmFetcher.ComputeHeadIdleWMB(df.fromBufferPartition.GetPartitionIdx())
		if !df.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			df.opts.logger.Debugw("skip publishing idle watermark",
				zap.Int("counter", df.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("idle", processorWMB.Idle))
			return
		}

		// if the validation passed, we will publish the watermark to all the toBuffer partitions.
		idlehandler.PublishIdleWatermark(ctx, df.toBuffer, df.wmPublisher, df.idleManager, df.opts.logger, dfv1.VertexTypeSink, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
		return
	}

	var dataMessages = make([]*isb.ReadMessage, 0, len(readMessages))

	// store the offsets of the messages we read from ISB
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range readMessages {
		readOffsets[idx] = m.ReadOffset
		if m.Kind == isb.Data {
			dataMessages = append(dataMessages, m)
		}
	}

	// fetch watermark if available
	// TODO: make it async (concurrent and wait later)
	// let's track only the first element's watermark. This is important because we reassign the watermark we fetch
	// to all the elements in the batch. If we were to assign last element's watermark, we will wrongly mark on-time data as late.
	// we fetch the watermark for the partition from which we read the message.
	processorWM := df.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, df.fromBufferPartition.GetPartitionIdx())

	var messageToStep []isb.Message

	// concurrent processing request channel
	processingCh := make(chan *readWriteMessagePair)
	// sinkResults stores the results after processing for all read messages. It indexes
	// a read message to the write message
	sinkResults := make([]readWriteMessagePair, len(dataMessages))

	// create a pool of processors
	var wg sync.WaitGroup
	for i := 0; i < df.opts.sinkConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			df.concurrentConvertReadToWriteMsgs(ctx, processingCh)
		}()
	}
	concurrentProcessingStart := time.Now()

	// only send the data messages
	for idx, m := range dataMessages {
		// emit message size metric
		readBytesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Add(float64(len(m.Payload)))
		// assign watermark to the message
		m.Watermark = time.Time(processorWM)
		// send message to the channel
		sinkResults[idx].readMessage = m
		processingCh <- &sinkResults[idx]
	}
	// let the go routines know that there is no more work
	close(processingCh)
	// wait till the processing is done. this will not be an infinite wait because the processing will exit if
	// context.Done() is closed.
	wg.Wait()
	df.opts.logger.Debugw("concurrent convert read message to write message completed", zap.Int("concurrency", df.opts.sinkConcurrency), zap.Duration("took", time.Since(concurrentProcessingStart)))

	// let's figure out which vertex to send the results to.
	// update the toBuffer(s) with writeMessages.
	for _, m := range sinkResults {
		// update toBuffer
		for _, message := range m.writeMessages {
			messageToStep = append(messageToStep, message.Message)
		}
	}

	// forward the message to the edge buffer (could be multiple edges)
	writeOffsets, err := df.writeToBuffer(ctx, df.toBuffer, messageToStep)
	if err != nil {
		df.opts.logger.Errorw("failed to write to toBuffer", zap.Error(err))
		df.fromBufferPartition.NoAck(ctx, readOffsets)
		return
	}
	df.opts.logger.Debugw("writeToBuffers completed")

	// in sink we don't drop any messages
	// so len(dataMessages) should be the same as len(writeOffsets)
	// if len(writeOffsets) is greater than 0, publish normal watermark
	// if len(writeOffsets) is 0, meaning we only have control messages,
	// we should not publish anything: the next len(readMessage) check will handle this idling situation
	if len(writeOffsets) > 0 {
		df.wmPublisher.PublishWatermark(processorWM, nil, int32(0))
		// reset because the toBuffer is no longer idling
		df.idleManager.Reset(df.toBuffer.GetName())
	}

	err = df.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		df.opts.logger.Errorw("Failed to ack from buffer", zap.Error(err))
		ackMessageError.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))
		return
	}
	ackMessagesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))

	// ProcessingTimes of the entire forwardAChunk
	forwardAChunkProcessingTime.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: df.fromBufferPartition.GetName()}).Observe(float64(time.Since(start).Microseconds()))
}

// ackFromBuffer acknowledges an array of offsets back to fromBufferPartition and is a blocking call or until shutdown has been initiated.
func (df *DataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) error {
	var ackRetryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0.1,
		Steps:    math.MaxInt,
		Duration: time.Millisecond * 10,
	}
	var ackOffsets = offsets
	attempt := 0

	ctxClosedErr := wait.ExponentialBackoff(ackRetryBackOff, func() (done bool, err error) {
		errs := df.fromBufferPartition.Ack(ctx, ackOffsets)
		attempt += 1
		summarizedErr := errorArrayToMap(errs)
		var failedOffsets []isb.Offset
		if len(summarizedErr) > 0 {
			df.opts.logger.Errorw("Failed to ack from buffer, retrying", zap.Any("errors", summarizedErr), zap.Int("attempt", attempt))
			// no point retrying if ctx.Done has been invoked
			select {
			case <-ctx.Done():
				// no point in retrying after we have been asked to stop.
				return false, ctx.Err()
			default:
				// retry only the failed offsets
				for i, offset := range ackOffsets {
					if errs[i] != nil {
						failedOffsets = append(failedOffsets, offset)
					}
				}
				ackOffsets = failedOffsets
				if ok, _ := df.IsShuttingDown(); ok {
					ackErr := fmt.Errorf("AckFromBuffer, Stop called while stuck on an internal error, %v", summarizedErr)
					return false, ackErr
				}
				return false, nil
			}
		} else {
			return true, nil
		}
	})

	if ctxClosedErr != nil {
		df.opts.logger.Errorw("Context closed while waiting to ack messages inside forward", zap.Error(ctxClosedErr))
	}

	return ctxClosedErr
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (df *DataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
	var (
		totalCount int
		writeCount int
		writeBytes float64
		dropBytes  float64
	)
	totalCount = len(messages)
	writeOffsets = make([]isb.Offset, 0, totalCount)

	for {
		_writeOffsets, errs := toBufferPartition.Write(ctx, messages)
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages []isb.Message
		needRetry := false
		for idx, msg := range messages {
			if err := errs[idx]; err != nil {
				// ATM there are no user defined errors during write, all are InternalErrors.
				if errors.As(err, &isb.NoRetryableBufferWriteErr{}) {
					// If toBufferPartition returns us a NoRetryableBufferWriteErr, we drop the message.
					dropBytes += float64(len(msg.Payload))
				} else {
					needRetry = true
					// we retry only failed messages
					failedMessages = append(failedMessages, msg)
					writeMessagesError.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Inc()
					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := df.IsShuttingDown(); ok {
						platformError.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName}).Inc()
						return writeOffsets, fmt.Errorf("writeToBuffer failed, Stop called while stuck on an internal error with failed messages:%d, %v", len(failedMessages), errs)
					}
				}
			} else {
				writeCount++
				writeBytes += float64(len(msg.Payload))
				// we support write offsets only for jetstream
				if _writeOffsets != nil {
					writeOffsets = append(writeOffsets, _writeOffsets[idx])
				}
			}
		}

		if needRetry {
			df.opts.logger.Errorw("Retrying failed messages",
				zap.Any("errors", errorArrayToMap(errs)),
				zap.String(metrics.LabelPipeline, df.pipelineName),
				zap.String(metrics.LabelVertex, df.vertexName),
				zap.String(metrics.LabelPartitionName, toBufferPartition.GetName()),
			)
			// set messages to failed for the retry
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(df.opts.retryInterval)
		} else {
			break
		}
	}

	dropMessagesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(totalCount - writeCount))
	dropBytesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(dropBytes)
	writeMessagesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(writeCount))
	writeBytesCount.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(writeBytes)
	return writeOffsets, nil
}

// concurrentConvertReadToWriteMsgs concurrently convert read messages to write messages
func (df *DataForward) concurrentConvertReadToWriteMsgs(_ context.Context, readMessagePair <-chan *readWriteMessagePair) {
	for message := range readMessagePair {
		writeMessages := []*isb.WriteMessage{{
			Message: message.readMessage.Message,
		}}
		message.writeMessages = append(message.writeMessages, writeMessages...)
	}
}

// errorArrayToMap summarizes an error array to map
func errorArrayToMap(errs []error) map[string]int64 {
	result := make(map[string]int64)
	for _, err := range errs {
		if err != nil {
			result[err.Error()]++
		}
	}
	return result
}

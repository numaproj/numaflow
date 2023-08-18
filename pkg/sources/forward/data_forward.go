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
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// DataForward reads data from source and forwards to inter-step buffers
type DataForward struct {
	// I have my reasons for overriding the default principle https://github.com/golang/go/issues/22602
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is a little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn context.CancelFunc
	// reader reads data from source.
	reader isb.BufferReader
	// toBuffers store the toVertex name to its owned buffers mapping.
	toBuffers          map[string][]isb.BufferWriter
	toWhichStepDecider forward.ToWhichStepDecider
	transformer        applier.MapApplier
	wmFetcher          fetch.Fetcher
	toVertexWMStores   map[string]store.WatermarkStore
	// toVertexWMPublishers stores the toVertex to publisher mapping.
	toVertexWMPublishers map[string]map[int32]publish.Publisher
	// srcWMPublisher is used to publish source watermark.
	srcWMPublisher isb.SourceWatermarkPublisher
	opts           options
	vertexName     string
	pipelineName   string
	// idleManager manages the idle watermark status.
	idleManager *wmb.IdleManager
	Shutdown
}

// NewDataForward creates a source data forwarder
func NewDataForward(
	vertex *dfv1.Vertex,
	fromStep isb.BufferReader,
	toSteps map[string][]isb.BufferWriter,
	toWhichStepDecider forward.ToWhichStepDecider,
	transformer applier.MapApplier,
	fetchWatermark fetch.Fetcher,
	srcWMPublisher isb.SourceWatermarkPublisher,
	toVertexWmStores map[string]store.WatermarkStore,
	opts ...Option) (*DataForward, error) {
	options := DefaultOptions()
	for _, o := range opts {
		if err := o(options); err != nil {
			return nil, err
		}
	}

	var toVertexWMPublishers = make(map[string]map[int32]publish.Publisher)
	for k := range toVertexWmStores {
		toVertexWMPublishers[k] = make(map[int32]publish.Publisher)
	}

	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	var isdf = DataForward{
		ctx:                  ctx,
		cancelFn:             cancel,
		reader:               fromStep,
		toBuffers:            toSteps,
		toWhichStepDecider:   toWhichStepDecider,
		transformer:          transformer,
		wmFetcher:            fetchWatermark,
		toVertexWMStores:     toVertexWmStores,
		toVertexWMPublishers: toVertexWMPublishers,
		srcWMPublisher:       srcWMPublisher,
		vertexName:           vertex.Spec.Name,
		pipelineName:         vertex.Spec.PipelineName,
		idleManager:          wmb.NewIdleManager(len(toSteps)),
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}
	// add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, options.logger)
	return &isdf, nil
}

// Start starts reading from source and forwards to the next buffers. Call `Stop` to stop.
func (isdf *DataForward) Start() <-chan struct{} {
	log := logging.FromContext(isdf.ctx)
	stopped := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Info("Starting forwarder...")
		// with wg approach can do more cleanup in case we need in the future.
		defer wg.Done()
		for {
			select {
			case <-isdf.ctx.Done():
				ok, err := isdf.IsShuttingDown()
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
				// shutdown the reader should be empty.
			}
			// keep doing what you are good at
			isdf.forwardAChunk(isdf.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// clean up resources for source reader and all the writers if any.
		if err := isdf.reader.Close(); err != nil {
			log.Errorw("Failed to close source reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed source reader", zap.String("sourceFrom", isdf.reader.GetName()))
		}
		for _, buffer := range isdf.toBuffers {
			for _, partition := range buffer {
				if err := partition.Close(); err != nil {
					log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", partition.GetName()))
				} else {
					log.Infow("Closed partition writer", zap.String("bufferTo", partition.GetName()))
				}
			}
		}

		// Close the fetcher and all the publishers.
		err := isdf.wmFetcher.Close()
		if err != nil {
			log.Error("Failed to close watermark fetcher, shutdown anyways...", zap.Error(err))
		}

		for _, toVertexPublishers := range isdf.toVertexWMPublishers {
			for _, pub := range toVertexPublishers {
				if err := pub.Close(); err != nil {
					log.Errorw("Failed to close publisher, shutdown anyways...", zap.Error(err))
				}
			}
		}

		close(stopped)
	}()
	return stopped
}

// readWriteMessagePair represents a read message and its processed (via transformer) write messages.
type readWriteMessagePair struct {
	readMessage      *isb.ReadMessage
	writeMessages    []*isb.WriteMessage
	transformerError error
}

// forwardAChunk forwards a chunk of message from the reader to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but do not include errors due to user code transformer, WhereTo, etc.
func (isdf *DataForward) forwardAChunk(ctx context.Context) {
	start := time.Now()
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during the restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := isdf.reader.Read(ctx, isdf.opts.readBatchSize)
	if err != nil {
		isdf.opts.logger.Warnw("failed to read from source", zap.Error(err))
		readMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Inc()
	}
	readMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Add(float64(len(readMessages)))

	// Process only if we have any read messages.
	// There is a natural looping here if there is an internal error while reading, and we are not able to proceed.
	if len(readMessages) == 0 {
		return
	}

	// store the offsets of the messages we read from source
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range readMessages {
		readOffsets[idx] = m.ReadOffset
	}

	// source data transformer applies filtering and assigns event time to source data, which doesn't require watermarks.
	// hence we assign time.UnixMilli(-1) to processorWM.
	processorWM := wmb.Watermark(time.UnixMilli(-1))

	var writeOffsets map[string][][]isb.Offset
	// create space for writeMessages specific to each step as we could forward to all the steps too.
	var messageToStep = make(map[string][][]isb.Message)
	for toVertex := range isdf.toBuffers {
		// over allocating to have a predictable pattern
		messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
	}

	// user defined transformer concurrent processing request channel
	transformerCh := make(chan *readWriteMessagePair)
	// transformerResults stores the results after user defined transformer processing for all read messages. It indexes
	// a read message to the corresponding write message
	transformerResults := make([]readWriteMessagePair, len(readMessages))
	// applyTransformer, if there is an Internal error, it is a blocking call and
	// will return only if shutdown has been initiated.

	// create a pool of Transformer Processors
	var wg sync.WaitGroup
	for i := 0; i < isdf.opts.transformerConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			isdf.concurrentApplyTransformer(ctx, transformerCh)
		}()
	}
	concurrentTransformerProcessingStart := time.Now()

	// send to transformer only the data messages
	for idx, m := range readMessages {
		// emit message size metric
		readBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Add(float64(len(m.Payload)))
		// assign watermark to the message
		m.Watermark = time.Time(processorWM)
		// send transformer processing work to the channel
		transformerResults[idx].readMessage = m
		transformerCh <- &transformerResults[idx]
	}
	// let the go routines know that there is no more work
	close(transformerCh)
	// wait till the processing is done. this will not be an infinite wait because the transformer processing will exit if
	// context.Done() is closed.
	wg.Wait()
	isdf.opts.logger.Debugw("concurrent applyTransformer completed", zap.Int("concurrency", isdf.opts.transformerConcurrency), zap.Duration("took", time.Since(concurrentTransformerProcessingStart)))
	concurrentTransformerProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Observe(float64(time.Since(concurrentTransformerProcessingStart).Microseconds()))
	// transformer processing is done.

	// publish source watermark and assign IsLate attribute based on new event time.
	var writeMessages []*isb.WriteMessage
	var transformedReadMessages []*isb.ReadMessage
	for _, m := range transformerResults {
		writeMessages = append(writeMessages, m.writeMessages...)
		for _, message := range m.writeMessages {
			// we convert each writeMessage to isb.ReadMessage by providing its parent ReadMessage's ReadOffset.
			// since we use message event time instead of the watermark to determine and publish source watermarks,
			// time.UnixMilli(-1) is assigned to the message watermark. transformedReadMessages are immediately
			// used below for publishing source watermarks.
			transformedReadMessages = append(transformedReadMessages, message.ToReadMessage(m.readMessage.ReadOffset, time.UnixMilli(-1)))
		}
	}
	// publish source watermark
	isdf.srcWMPublisher.PublishSourceWatermarks(transformedReadMessages)
	// fetch the source watermark again, we might not get the latest watermark because of publishing delay,
	// but ideally we should use the latest to determine the IsLate attribute.
	processorWM = isdf.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, isdf.reader.GetPartitionIdx())
	// assign isLate
	for _, m := range writeMessages {
		if processorWM.After(m.EventTime) { // Set late data at source level
			m.IsLate = true
		}
	}

	var sourcePartitionsIndices = make(map[int32]bool)
	// let's figure out which vertex to send the results to.
	// update the toBuffer(s) with writeMessages.
	for _, m := range transformerResults {
		// Look for errors in transformer processing if we see even 1 error we return.
		// Handling partial retrying is not worth ATM.
		if m.transformerError != nil {
			transformerError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Inc()
			isdf.opts.logger.Errorw("failed to apply source transformer", zap.Error(m.transformerError))
			return
		}
		// update toBuffers
		for _, message := range m.writeMessages {
			if err := isdf.whereToStep(message, messageToStep, m.readMessage); err != nil {
				isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
				return
			}
		}
		// get the list of source partitions for which we have read messages, we will use this to publish watermarks to toVertices
		sourcePartitionsIndices[m.readMessage.ReadOffset.PartitionIdx()] = true
	}

	// forward the messages to the edge buffer (could be multiple edges)
	writeOffsets, err = isdf.writeToBuffers(ctx, messageToStep)
	if err != nil {
		isdf.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
		return
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// It's used to determine which buffers should receive an idle watermark.
	// It is created as a slice because it tracks per partition activity info.
	var activeWatermarkBuffers = make(map[string][]bool)
	// forward the highest watermark to all the edges to avoid idle edge problem
	// TODO: sort and get the highest value
	for toVertexName, toVertexBufferOffsets := range writeOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(toVertexBufferOffsets))
		if vertexPublishers, ok := isdf.toVertexWMPublishers[toVertexName]; ok {
			for index, offsets := range toVertexBufferOffsets {
				if len(offsets) > 0 {
					// publish watermark to all the source partitions
					// create a publisher if it doesn't exist, we don't want to publish to all the partitions
					for sp := range sourcePartitionsIndices {
						var publisher, ok = vertexPublishers[sp]
						if !ok {
							publisher = isdf.createToVertexWatermarkPublisher(toVertexName, sp)
							vertexPublishers[sp] = publisher
						}

						publisher.PublishWatermark(processorWM, offsets[len(offsets)-1], int32(index))
						activeWatermarkBuffers[toVertexName][index] = true
						// reset because the toBuffer partition is no longer idling
						isdf.idleManager.Reset(isdf.toBuffers[toVertexName][index].GetName())
					}
				}
				// This (len(offsets) == 0) happens at conditional forwarding, there's no data written to the buffer
			}
		}
	}

	// condition "len(activeWatermarkBuffers) < len(isdf.toVertexWMPublishers)":
	// send idle watermark only if we have idled out buffers
	for toVertexName := range isdf.toVertexWMPublishers {
		for index, activePartition := range activeWatermarkBuffers[toVertexName] {
			if !activePartition {
				// use the watermark of the current read batch for the idle watermark
				// same as read len==0 because there's no event published to the buffer
				if vertexPublishers, ok := isdf.toVertexWMPublishers[toVertexName]; ok {
					for sp := range sourcePartitionsIndices {
						var publisher, ok = vertexPublishers[sp]
						if !ok {
							publisher = isdf.createToVertexWatermarkPublisher(toVertexName, sp)
							vertexPublishers[sp] = publisher
						}
						idlehandler.PublishIdleWatermark(ctx, isdf.toBuffers[toVertexName][index], publisher, isdf.idleManager, isdf.opts.logger, dfv1.VertexTypeSource, processorWM)
					}
				}
			}
		}
	}

	// when we apply transformer, we don't handle partial errors (it's either non or all, non will return early),
	// so we should be able to ack all the readOffsets including data messages and control messages
	err = isdf.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		isdf.opts.logger.Errorw("failed to ack from source", zap.Error(err))
		ackMessageError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Add(float64(len(readOffsets)))
		return
	}
	ackMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Add(float64(len(readOffsets)))

	// ProcessingTimes of the entire forwardAChunk
	forwardAChunkProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Observe(float64(time.Since(start).Microseconds()))
}

// ackFromBuffer acknowledges an array of offsets back to the reader
// and is a blocking call or until shutdown has been initiated.
func (isdf *DataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) error {
	var ackRetryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0.1,
		Steps:    math.MaxInt,
		Duration: time.Millisecond * 10,
	}
	var ackOffsets = offsets
	attempt := 0

	ctxClosedErr := wait.ExponentialBackoff(ackRetryBackOff, func() (done bool, err error) {
		errs := isdf.reader.Ack(ctx, ackOffsets)
		attempt += 1
		summarizedErr := errorArrayToMap(errs)
		var failedOffsets []isb.Offset
		if len(summarizedErr) > 0 {
			isdf.opts.logger.Errorw("Failed to ack from buffer, retrying", zap.Any("errors", summarizedErr), zap.Int("attempt", attempt))
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
				if ok, _ := isdf.IsShuttingDown(); ok {
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
		isdf.opts.logger.Errorw("Context closed while waiting to ack messages inside forward", zap.Error(ctxClosedErr))
	}

	return ctxClosedErr
}

// writeToBuffers is a blocking call until all the messages have been forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (isdf *DataForward) writeToBuffers(
	ctx context.Context, messageToStep map[string][][]isb.Message,
) (writeOffsets map[string][][]isb.Offset, err error) {
	// messageToStep contains all the to buffers, so the messages could be empty (conditional forwarding).
	// So writeOffsets also contains all the to buffers, but the returned offsets might be empty.
	writeOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexMessages := range messageToStep {
		writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexMessages))
	}
	for toVertexName, toVertexBuffer := range isdf.toBuffers {
		for index, partition := range toVertexBuffer {
			writeOffsets[toVertexName][index], err = isdf.writeToBuffer(ctx, partition, messageToStep[toVertexName][index])
			if err != nil {
				return nil, err
			}
		}
	}
	return writeOffsets, nil
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (isdf *DataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
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
				// ATM there are no user-defined errors during writing, all are InternalErrors.
				if errors.As(err, &isb.NoRetryableBufferWriteErr{}) {
					// If toBufferPartition returns us a NoRetryableBufferWriteErr, we drop the message.
					dropBytes += float64(len(msg.Payload))
				} else {
					needRetry = true
					// we retry only failed messages
					failedMessages = append(failedMessages, msg)
					writeMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Inc()
					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := isdf.IsShuttingDown(); ok {
						platformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName}).Inc()
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
			isdf.opts.logger.Errorw("Retrying failed messages",
				zap.Any("errors", errorArrayToMap(errs)),
				zap.String(metrics.LabelPipeline, isdf.pipelineName),
				zap.String(metrics.LabelVertex, isdf.vertexName),
				zap.String(metrics.LabelPartitionName, toBufferPartition.GetName()),
			)
			// set messages to the failed slice for the retry
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
		} else {
			break
		}
	}

	dropMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(totalCount - writeCount))
	dropBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(dropBytes)
	writeMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(writeCount))
	writeBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(writeBytes)
	return writeOffsets, nil
}

// concurrentApplyTransformer applies the transformer based on the request from the channel
func (isdf *DataForward) concurrentApplyTransformer(ctx context.Context, readMessagePair <-chan *readWriteMessagePair) {
	for message := range readMessagePair {
		start := time.Now()
		transformerReadMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Inc()
		writeMessages, err := isdf.applyTransformer(ctx, message.readMessage)
		transformerWriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Add(float64(len(writeMessages)))
		message.writeMessages = append(message.writeMessages, writeMessages...)
		message.transformerError = err
		transformerProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.reader.GetName()}).Observe(float64(time.Since(start).Microseconds()))
	}
}

// applyTransformer applies the transformer and will block if there is any InternalErr. On the other hand, if this is a UserError
// the skip flag is set. The ShutDown flag will only if there is an InternalErr and ForceStop has been invoked.
// The UserError retry will be done on the applyTransformer.
func (isdf *DataForward) applyTransformer(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	for {
		writeMessages, err := isdf.transformer.ApplyMap(ctx, readMessage)
		if err != nil {
			isdf.opts.logger.Errorw("Transformer.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform, we should not lose or corrupt data.
			// this does not mean we should prohibit this from a shutdown.
			if ok, _ := isdf.IsShuttingDown(); ok {
				isdf.opts.logger.Errorw("Transformer.Apply, Stop called while stuck on an internal error", zap.Error(err))
				platformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName}).Inc()
				return nil, err
			}
			continue
		} else {
			// if we do not get a time from Transformer, we set it to the time from (N-1)th vertex
			for index, m := range writeMessages {
				m.ID = fmt.Sprintf("%s-%s-%d", readMessage.ReadOffset.String(), isdf.vertexName, index)
				if m.EventTime.IsZero() {
					m.EventTime = readMessage.EventTime
				}
			}
			return writeMessages, nil
		}
	}
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *DataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors
	to, err := isdf.toWhichStepDecider.WhereTo(writeMessage.Keys, writeMessage.Tags)
	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.reader.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := isdf.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			platformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName}).Inc()
			return err
		}
		return err
	}

	for _, t := range to {
		if _, ok := messageToStep[t.ToVertexName]; !ok {
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.reader.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName)}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = append(messageToStep[t.ToVertexName][t.ToVertexPartitionIdx], writeMessage.Message)
	}
	return nil
}

// createToVertexWatermarkPublisher creates a watermark publisher for the given toVertexName and partition
func (isdf *DataForward) createToVertexWatermarkPublisher(toVertexName string, partition int32) publish.Publisher {

	wmStore := isdf.toVertexWMStores[toVertexName]
	entityName := fmt.Sprintf("%s-%s-%d", isdf.pipelineName, isdf.vertexName, partition)
	processorEntity := processor.NewProcessorEntity(entityName)

	publisher := publish.NewPublish(isdf.ctx, processorEntity, wmStore, int32(len(isdf.toBuffers[toVertexName])))
	isdf.toVertexWMPublishers[toVertexName][partition] = publisher
	return publisher
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

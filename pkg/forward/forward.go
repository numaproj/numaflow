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

/*
Package forward does the Read (fromBufferPartition) -> Process (UDF) -> Forward (toBuffers) -> Ack (fromBufferPartition) loop.
*/
package forward

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// InterStepDataForward forwards the data from previous step to the current step via inter-step buffer.
type InterStepDataForward struct {
	// I have my reasons for overriding the default principle https://github.com/golang/go/issues/22602
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn            context.CancelFunc
	fromBufferPartition isb.BufferReader
	// toBuffers is a map of toVertex name to the toVertex's owned buffers.
	toBuffers map[string][]isb.BufferWriter
	FSD       ToWhichStepDecider
	UDF       applier.MapApplier
	wmFetcher fetch.Fetcher
	// wmPublishers stores the vertex to publisher mapping
	wmPublishers map[string]publish.Publisher
	opts         options
	vertexName   string
	pipelineName string
	// idleManager manages the idle watermark status.
	idleManager *wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid.
	wmbChecker wmb.WMBChecker
	Shutdown
}

// NewInterStepDataForward creates an inter-step forwarder.
func NewInterStepDataForward(vertex *dfv1.Vertex,
	fromStep isb.BufferReader,
	toSteps map[string][]isb.BufferWriter,
	fsd ToWhichStepDecider,
	applyUDF applier.MapApplier,
	fetchWatermark fetch.Fetcher,
	publishWatermark map[string]publish.Publisher,
	opts ...Option) (*InterStepDataForward, error) {

	options := DefaultOptions()
	for _, o := range opts {
		if err := o(options); err != nil {
			return nil, err
		}
	}
	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	var isdf = InterStepDataForward{
		ctx:                 ctx,
		cancelFn:            cancel,
		fromBufferPartition: fromStep,
		toBuffers:           toSteps,
		FSD:                 fsd,
		UDF:                 applyUDF,
		wmFetcher:           fetchWatermark,
		wmPublishers:        publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:   vertex.Spec.Name,
		pipelineName: vertex.Spec.PipelineName,
		idleManager:  wmb.NewIdleManager(len(toSteps)),
		wmbChecker:   wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}

	// Add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, options.logger)

	if isdf.opts.vertexType == dfv1.VertexTypeSource {
		if isdf.opts.srcWatermarkPublisher == nil {
			return nil, fmt.Errorf("failed to assign a non-nil source watermark publisher for source vertex data forwarder")
		}
		if isdf.opts.enableMapUdfStream {
			return nil, fmt.Errorf("stream is not supported for source data transformer")
		}
	}

	if isdf.opts.enableMapUdfStream && isdf.opts.readBatchSize != 1 {
		return nil, fmt.Errorf("batch size is not 1 with UDF streaming")
	}

	return &isdf, nil
}

// Start starts reading the buffer and forwards to the next buffers. Call `Stop` to stop.
func (isdf *InterStepDataForward) Start() <-chan struct{} {
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
				// shutdown the fromBufferPartition should be empty.
			}
			// keep doing what you are good at
			isdf.forwardAChunk(isdf.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// Clean up resources for buffer reader and all the writers if any.
		if err := isdf.fromBufferPartition.Close(); err != nil {
			log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed buffer reader", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()))
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

		// stop watermark fetcher
		if err := isdf.wmFetcher.Close(); err != nil {
			log.Errorw("Failed to close watermark fetcher", zap.Error(err))
		}

		// stop watermark publisher
		for _, publisher := range isdf.wmPublishers {
			if err := publisher.Close(); err != nil {
				log.Errorw("Failed to close watermark publisher", zap.Error(err))
			}
		}
		close(stopped)
	}()

	return stopped
}

// readWriteMessagePair represents a read message and its processed (via UDF) write messages.
type readWriteMessagePair struct {
	readMessage   *isb.ReadMessage
	writeMessages []*isb.WriteMessage
	udfError      error
}

// forwardAChunk forwards a chunk of message from the fromBufferPartition to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to user code UDFs, WhereTo, etc.
func (isdf *InterStepDataForward) forwardAChunk(ctx context.Context) {
	start := time.Now()
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := isdf.fromBufferPartition.Read(ctx, isdf.opts.readBatchSize)
	if err != nil {
		isdf.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
		readMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
	}
	readMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readMessages)))

	// process only if we have any read messages. There is a natural looping here if there is an internal error while
	// reading, and we are not able to proceed.
	if len(readMessages) == 0 {
		// When the read length is zero, the write length is definitely zero too
		// meaning there's no data to be published to the next vertex, and we consider this
		// situation as idling.
		// In order to continue propagating watermark, we will set watermark idle=true and publish it.
		// We also publish a control message if this is the first time we get this idle situation.
		// we use the HeadWMB as the watermark for the idle
		// we get the HeadWMB for the partition from which we read the messages
		var processorWMB = isdf.wmFetcher.GetHeadWMB(isdf.fromBufferPartition.GetPartitionIdx())
		if !isdf.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			isdf.opts.logger.Debugw("skip publishing idle watermark",
				zap.Int("counter", isdf.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("Idle", processorWMB.Idle))
			return
		}

		// if the validation passed, we will publish the watermark to all the toBuffer partitions.
		for toVertexName, toVertexBuffer := range isdf.toBuffers {
			for _, partition := range toVertexBuffer {
				if p, ok := isdf.wmPublishers[toVertexName]; ok {
					idlehandler.PublishIdleWatermark(ctx, partition, p, isdf.idleManager, isdf.opts.logger, isdf.opts.vertexType, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
				}
			}
		}
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

	var processorWM wmb.Watermark
	if isdf.opts.vertexType == dfv1.VertexTypeSource {
		// for source vertex, the udf is the source data transformer.
		// in this case, we assign time.UnixMilli(-1) to processorWM.
		// source data transformer applies filtering and assigns event time to source data, which doesn't require watermarks.
		processorWM = wmb.Watermark(time.UnixMilli(-1))
	} else {
		// fetch watermark if available
		// TODO: make it async (concurrent and wait later)
		// let's track only the first element's watermark. This is important because we reassign the watermark we fetch
		// to all the elements in the batch. If we were to assign last element's watermark, we will wrongly mark on-time data as late.
		// we fetch the watermark for the partition from which we read the message.
		processorWM = isdf.wmFetcher.GetWatermark(readMessages[0].ReadOffset, isdf.fromBufferPartition.GetPartitionIdx())
	}

	var writeOffsets map[string][][]isb.Offset
	if !isdf.opts.enableMapUdfStream {
		// create space for writeMessages specific to each step as we could forward to all the steps too.
		var messageToStep = make(map[string][][]isb.Message)
		for toVertex := range isdf.toBuffers {
			// over allocating to have a predictable pattern
			messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
		}

		// udf concurrent processing request channel
		udfCh := make(chan *readWriteMessagePair)
		// udfResults stores the results after UDF processing for all read messages. It indexes
		// a read message to the corresponding write message
		udfResults := make([]readWriteMessagePair, len(dataMessages))
		// applyUDF, if there is an Internal error it is a blocking call and will return only if shutdown has been initiated.

		// create a pool of UDF Processors
		var wg sync.WaitGroup
		for i := 0; i < isdf.opts.udfConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				isdf.concurrentApplyUDF(ctx, udfCh)
			}()
		}
		concurrentUDFProcessingStart := time.Now()

		// send to UDF only the data messages
		for idx, m := range dataMessages {
			// emit message size metric
			readBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(m.Payload)))
			// assign watermark to the message. assign time.UnixMilli(-1) as watermark when we are at source vertex.
			m.Watermark = time.Time(processorWM)
			// send UDF processing work to the channel
			udfResults[idx].readMessage = m
			udfCh <- &udfResults[idx]
		}
		// let the go routines know that there is no more work
		close(udfCh)
		// wait till the processing is done. this will not be an infinite wait because the UDF processing will exit if
		// context.Done() is closed.
		wg.Wait()
		isdf.opts.logger.Debugw("concurrent applyUDF completed", zap.Int("concurrency", isdf.opts.udfConcurrency), zap.Duration("took", time.Since(concurrentUDFProcessingStart)))
		concurrentUDFProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Observe(float64(time.Since(concurrentUDFProcessingStart).Microseconds()))
		// UDF processing is done.

		// if vertex type is source, it means we have finished the source data transformation.
		// let's publish source watermark and assign IsLate attribute based on new event time.
		if isdf.opts.vertexType == dfv1.VertexTypeSource {
			var writeMessages []*isb.WriteMessage
			var transformedReadMessages []*isb.ReadMessage
			for _, m := range udfResults {
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
			isdf.opts.srcWatermarkPublisher.PublishSourceWatermarks(transformedReadMessages)
			// fetch the source watermark again, we might not get the latest watermark because of publishing delay,
			// but ideally we should use the latest to determine the IsLate attribute.
			processorWM = isdf.wmFetcher.GetWatermark(readMessages[0].ReadOffset, isdf.fromBufferPartition.GetPartitionIdx())
			// assign isLate
			for _, m := range writeMessages {
				if processorWM.After(m.EventTime) { // Set late data at source level
					m.IsLate = true
				}
			}
		}

		// let's figure out which vertex to send the results to.
		// update the toBuffer(s) with writeMessages.
		for _, m := range udfResults {
			// look for errors in udf processing, if we see even 1 error NoAck all messages
			// then return. Handling partial retrying is not worth ATM.
			if m.udfError != nil {
				udfError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
				isdf.opts.logger.Errorw("failed to applyUDF", zap.Error(m.udfError))
				// As there's no partial failure, non-ack all the readOffsets
				isdf.fromBufferPartition.NoAck(ctx, readOffsets)
				return
			}
			// update toBuffers
			for _, message := range m.writeMessages {
				if err := isdf.whereToStep(message, messageToStep, m.readMessage); err != nil {
					isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
					isdf.fromBufferPartition.NoAck(ctx, readOffsets)
					return
				}
			}
		}

		// forward the message to the edge buffer (could be multiple edges)
		writeOffsets, err = isdf.writeToBuffers(ctx, messageToStep)
		if err != nil {
			isdf.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
			isdf.fromBufferPartition.NoAck(ctx, readOffsets)
			return
		}
	} else {
		writeOffsets, err = isdf.streamMessage(ctx, dataMessages, processorWM)
		if err != nil {
			isdf.opts.logger.Errorw("failed to streamMessage", zap.Error(err))
			// As there's no partial failure, non-ack all the readOffsets
			isdf.fromBufferPartition.NoAck(ctx, readOffsets)
			return
		}
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// It is created as a slice because it tracks per partition activity info.
	var activeWatermarkBuffers = make(map[string][]bool)
	// forward the highest watermark to all the edges to avoid idle edge problem
	// TODO: sort and get the highest value
	for toVertexName, toVertexBufferOffsets := range writeOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(toVertexBufferOffsets))
		if publisher, ok := isdf.wmPublishers[toVertexName]; ok {
			for index, offsets := range toVertexBufferOffsets {
				if isdf.opts.vertexType == dfv1.VertexTypeSource || isdf.opts.vertexType == dfv1.VertexTypeMapUDF ||
					isdf.opts.vertexType == dfv1.VertexTypeReduceUDF {
					if len(offsets) > 0 {
						publisher.PublishWatermark(processorWM, offsets[len(offsets)-1], int32(index))
						activeWatermarkBuffers[toVertexName][index] = true
						// reset because the toBuffer partition is no longer idling
						isdf.idleManager.Reset(isdf.toBuffers[toVertexName][index].GetName())
					}
					// This (len(offsets) == 0) happens at conditional forwarding, there's no data written to the buffer
				} else { // For Sink vertex, and it does not care about the offset during watermark publishing
					publisher.PublishWatermark(processorWM, nil, int32(index))
					activeWatermarkBuffers[toVertexName][index] = true
					// reset because the toBuffer partition is no longer idling
					isdf.idleManager.Reset(isdf.toBuffers[toVertexName][index].GetName())
				}
			}
		}
	}
	// - condition1 "len(dataMessages) > 0" :
	//   Meaning, we do have some data messages, but we may not have written to all out buffers or its partitions.
	//   It could be all data messages are dropped, or conditional forwarding to part of the out buffers.
	//   If we don't have this condition check, when dataMessages is zero but ctrlMessages > 0, we will
	//   wrongly publish an idle watermark without the ctrl message and the ctrl message tracking map.
	// - condition 2 "len(activeWatermarkBuffers) < len(isdf.wmPublishers)" :
	//   send idle watermark only if we have idle out buffers
	// Note: When the len(dataMessages) is 0, meaning all the readMessages are control messages, we choose not to do extra steps
	// This is because, if the idle continues, we will eventually handle the idle watermark when we read the next batch where the len(readMessages) will be zero
	if len(dataMessages) > 0 {
		for bufferName := range isdf.wmPublishers {
			for index, activePartition := range activeWatermarkBuffers[bufferName] {
				if !activePartition {
					// use the watermark of the current read batch for the idle watermark
					// same as read len==0 because there's no event published to the buffer
					if p, ok := isdf.wmPublishers[bufferName]; ok {
						idlehandler.PublishIdleWatermark(ctx, isdf.toBuffers[bufferName][index], p, isdf.idleManager, isdf.opts.logger, isdf.opts.vertexType, processorWM)
					}
				}
			}
		}
	}

	// when we apply udf, we don't handle partial errors (it's either non or all, non will return early),
	// so we should be able to ack all the readOffsets including data messages and control messages
	err = isdf.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		isdf.opts.logger.Errorw("failed to ack from buffer", zap.Error(err))
		ackMessageError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))
		return
	}
	ackMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))

	// ProcessingTimes of the entire forwardAChunk
	forwardAChunkProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Observe(float64(time.Since(start).Microseconds()))
}

// streamMessage streams the data messages to the next step.
func (isdf *InterStepDataForward) streamMessage(
	ctx context.Context,
	dataMessages []*isb.ReadMessage,
	processorWM wmb.Watermark,
) (map[string][][]isb.Offset, error) {
	// create space for writeMessages specific to each step as we could forward to all the steps too.
	// these messages are for per partition (due to round-robin writes) for load balancing
	var messageToStep = make(map[string][][]isb.Message)
	var writeOffsets = make(map[string][][]isb.Offset)

	for toVertex := range isdf.toBuffers {
		// over allocating to have a predictable pattern
		messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
		writeOffsets[toVertex] = make([][]isb.Offset, len(isdf.toBuffers[toVertex]))
	}

	if len(dataMessages) > 1 {
		errMsg := "data message size is not 1 with UDF streaming"
		isdf.opts.logger.Errorw(errMsg)
		return nil, fmt.Errorf(errMsg)
	} else if len(dataMessages) == 1 {
		// send to UDF only the data messages

		// emit message size metric
		readBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).
			Add(float64(len(dataMessages[0].Payload)))
		// assign watermark to the message. assign time.UnixMilli(-1) as watermark when we are at source vertex.
		dataMessages[0].Watermark = time.Time(processorWM)

		// process the UDF and get the result
		start := time.Now()
		udfReadMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()

		writeMessageCh := make(chan isb.WriteMessage)
		errs, ctx := errgroup.WithContext(ctx)
		errs.Go(func() error {
			return isdf.UDF.ApplyMapStream(ctx, dataMessages[0], writeMessageCh)
		})

		// Stream the message to the next vertex. First figure out which vertex
		// to send the result to. Then update the toBuffer(s) with writeMessage.
		msgIndex := 0
		for writeMessage := range writeMessageCh {
			// add partition to the ID, this is to make sure that the ID is unique across partitions
			writeMessage.ID = fmt.Sprintf("%s-%d-%d", dataMessages[0].ReadOffset.String(), isdf.fromBufferPartition.GetPartitionIdx(), msgIndex)
			msgIndex += 1
			udfWriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(1))

			// update toBuffers
			if err := isdf.whereToStep(&writeMessage, messageToStep, dataMessages[0]); err != nil {
				return nil, fmt.Errorf("failed at whereToStep, error: %w", err)
			}

			// Forward the message to the edge buffer (could be multiple edges)
			curWriteOffsets, err := isdf.writeToBuffers(ctx, messageToStep)
			if err != nil {
				return nil, fmt.Errorf("failed to write to toBuffers, error: %w", err)
			}
			// Merge curWriteOffsets into writeOffsets
			for vertexName, toVertexBufferOffsets := range curWriteOffsets {
				for index, offsets := range toVertexBufferOffsets {
					writeOffsets[vertexName][index] = append(writeOffsets[vertexName][index], offsets...)
				}
			}
		}

		// look for errors in udf processing, if we see even 1 error NoAck all messages
		// then return. Handling partial retrying is not worth ATM.
		if err := errs.Wait(); err != nil {
			udfError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName,
				metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
			// We do not retry as we are streaming
			if ok, _ := isdf.IsShuttingDown(); ok {
				isdf.opts.logger.Errorw("UDF.Apply, Stop called while stuck on an internal error", zap.Error(err))
				platformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName}).Inc()
			}
			return nil, fmt.Errorf("failed to applyUDF, error: %w", err)
		}

		udfProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName,
			metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Observe(float64(time.Since(start).Microseconds()))
	} else {
		// Even not data messages, forward the message to the edge buffer (could be multiple edges)
		var err error
		writeOffsets, err = isdf.writeToBuffers(ctx, messageToStep)
		if err != nil {
			return nil, fmt.Errorf("failed to write to toBuffers, error: %w", err)
		}
	}

	return writeOffsets, nil
}

// ackFromBuffer acknowledges an array of offsets back to fromBufferPartition and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) error {
	var ackRetryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0.1,
		Steps:    math.MaxInt,
		Duration: time.Millisecond * 10,
	}
	var ackOffsets = offsets
	attempt := 0

	ctxClosedErr := wait.ExponentialBackoff(ackRetryBackOff, func() (done bool, err error) {
		errs := isdf.fromBufferPartition.Ack(ctx, ackOffsets)
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

// writeToBuffers is a blocking call until all the messages have be forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (isdf *InterStepDataForward) writeToBuffers(
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
func (isdf *InterStepDataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
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
			// set messages to failed for the retry
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

// concurrentApplyUDF applies the UDF based on the request from the channel
func (isdf *InterStepDataForward) concurrentApplyUDF(ctx context.Context, readMessagePair <-chan *readWriteMessagePair) {
	for message := range readMessagePair {
		start := time.Now()
		udfReadMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
		writeMessages, err := isdf.applyUDF(ctx, message.readMessage)
		udfWriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(writeMessages)))
		message.writeMessages = append(message.writeMessages, writeMessages...)
		message.udfError = err
		udfProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Observe(float64(time.Since(start).Microseconds()))
	}
}

// applyUDF applies the UDF and will block if there is any InternalErr. On the other hand, if this is a UserError
// the skip flag is set. ShutDown flag will only if there is an InternalErr and ForceStop has been invoked.
// The UserError retry will be done on the ApplyUDF.
func (isdf *InterStepDataForward) applyUDF(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	for {
		writeMessages, err := isdf.UDF.ApplyMap(ctx, readMessage)
		if err != nil {
			isdf.opts.logger.Errorw("UDF.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform we should not lose or corrupt data.
			// this does not mean we should prohibit this from a shutdown.
			if ok, _ := isdf.IsShuttingDown(); ok {
				isdf.opts.logger.Errorw("UDF.Apply, Stop called while stuck on an internal error", zap.Error(err))
				platformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName}).Inc()
				return nil, err
			}
			continue
		} else {
			// if we do not get a time from UDF, we set it to the time from (N-1)th vertex
			for index, m := range writeMessages {
				// add partition to the ID, this is to make sure that the ID is unique across partitions
				m.ID = fmt.Sprintf("%s-%d-%d", readMessage.ReadOffset.String(), isdf.fromBufferPartition.GetPartitionIdx(), index)
				if m.EventTime.IsZero() {
					m.EventTime = readMessage.EventTime
				}
			}
			return writeMessages, nil
		}
	}
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *InterStepDataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors
	to, err := isdf.FSD.WhereTo(writeMessage.Keys, writeMessage.Tags)
	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
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
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName)}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = append(messageToStep[t.ToVertexName][t.ToVertexPartitionIdx], writeMessage.Message)
	}
	return nil
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

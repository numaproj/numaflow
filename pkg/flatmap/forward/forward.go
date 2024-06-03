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
Package forward does the Read (fromBufferPartition) -> Process (map UDF) -> Forward (toBuffers) -> Ack (fromBufferPartition) loop.
*/
package forward

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/flatmap/forward/applier"
	"github.com/numaproj/numaflow/pkg/flatmap/types"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
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
	toBuffers  map[string][]isb.BufferWriter
	FSD        forwarder.ToWhichStepDecider
	flatmapUDF applier.FlatmapApplier
	wmFetcher  fetch.Fetcher
	// wmPublishers stores the vertex to publisher mapping
	wmPublishers  map[string]publish.Publisher
	opts          options
	vertexName    string
	pipelineName  string
	vertexReplica int32
	// idleManager manages the idle watermark status.
	idleManager wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid when the len(readMessage) is 0.
	wmbChecker wmb.WMBChecker
	Shutdown
}

// NewInterStepDataForward creates an inter-step forwarder.
func NewInterStepDataForward(vertexInstance *dfv1.VertexInstance, fromStep isb.BufferReader, toSteps map[string][]isb.BufferWriter, fsd forwarder.ToWhichStepDecider, applyUDF applier.FlatmapApplier, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, idleManager wmb.IdleManager, opts ...Option) (*InterStepDataForward, error) {

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
		flatmapUDF:          applyUDF,
		wmFetcher:           fetchWatermark,
		wmPublishers:        publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		idleManager:   idleManager,
		wmbChecker:    wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}

	// Add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, options.logger)

	if isdf.opts.enableMapUdfStream && isdf.opts.readBatchSize != 1 {
		return nil, fmt.Errorf("batch size is not 1 with map UDF streaming")
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

		close(stopped)
	}()

	return stopped
}

// readWriteMessagePair represents a read message and its processed (via map UDF) write messages.
type readWriteMessagePair struct {
	readMessage   *isb.ReadMessage
	writeMessages []*isb.WriteMessage
	udfError      error
}

// forwardAChunk forwards a chunk of messages from the fromBufferPartition to the toBuffers.
// It does the Read -> Process -> Forward -> Ack chain for a chunk of messages returned by the first Read call.
// It will return only if it was able to process all the message read after forwarding, barring any platform errors.
// The platform errors include buffer-full, buffer-not-reachable, etc., but does not include errors due to user code UDFs, WhereTo, etc.
// Internally, the UDF processing, write and ack are functioning asynchronously, which is done to help with a situation where
// a given message takes a long time to process, thus it does not block the other messages in this case.
// Though, we would still wait for the whole chain to complete till ack for all the messages before moving on to the
// next batch
func (isdf *InterStepDataForward) forwardAChunk(ctx context.Context) {
	// start time for processing forwardAChunk
	start := time.Now()

	// Step 1: Read messages from the ISB

	// we read from the ISB buffer for the fromBufferPartition <= isdf.opts.readBatchSize number of messages.
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := isdf.fromBufferPartition.Read(ctx, isdf.opts.readBatchSize)
	isdf.opts.logger.Debugw("Read from buffer", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()), zap.Int64("length", int64(len(readMessages))))
	if err != nil {
		// TODO(stream): if we are not able to read, should we have a retry? Dont see in the code path
		isdf.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
		metrics.ReadMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
	}

	// TODO(stream): check for idle watermark here Now that we are reading just a batch, we should be able
	//  to publish watermark similar to the current map logic. Revisit once, happy path/error path is done
	//// process only if we have any read messages. There is a natural looping here if there is an internal error while
	//// reading, and we are not able to proceed.
	//if len(readMessages) == 0 {
	//	// When the read length is zero, the write length is definitely zero too,
	//	// meaning there's no data to be published to the next vertex, and we consider this
	//	// situation as idling.
	//	// In order to continue propagating watermark, we will set watermark idle=true and publish it.
	//	// We also publish a control message if this is the first time we get this idle situation.
	//	// We compute the HeadIdleWMB using the given partition as the idle watermark
	//	var processorWMB = isdf.wmFetcher.ComputeHeadIdleWMB(isdf.fromBufferPartition.GetPartitionIdx())
	//	if !isdf.wmbChecker.ValidateHeadWMB(processorWMB) {
	//		// validation failed, skip publishing
	//		isdf.opts.logger.Debugw("skip publishing idle watermark",
	//			zap.Int("counter", isdf.wmbChecker.GetCounter()),
	//			zap.Int64("offset", processorWMB.Offset),
	//			zap.Int64("watermark", processorWMB.Watermark),
	//			zap.Bool("idle", processorWMB.Idle))
	//		return
	//	}
	//
	//	// if the validation passed, we will publish the watermark to all the toBuffer partitions.
	//	for toVertexName, toVertexBuffer := range isdf.toBuffers {
	//		for _, partition := range toVertexBuffer {
	//			if p, ok := isdf.wmPublishers[toVertexName]; ok {
	//				idlehandler.PublishIdleWatermark(ctx, isdf.fromBufferPartition.GetPartitionIdx(), partition, p, isdf.idleManager, isdf.opts.logger, isdf.vertexName, isdf.pipelineName, dfv1.VertexTypeMapUDF, isdf.vertexReplica, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
	//			}
	//		}
	//	}
	//	return
	//}
	// TODO(stream): remove once idle watermark is done
	if len(readMessages) == 0 {
		return
	}

	// TODO(stream): see if that can be optimised by not duplicating the data slice, and passing
	// We send only the dataMessages to the UDF for processing, for the non data messages,
	// we just need to ack?
	var dataMessages = make([]*isb.ReadMessage, 0, len(readMessages))
	// the readMessages itself store the offsets of the messages we read from ISB
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range readMessages {
		readOffsets[idx] = m.ReadOffset
		if m.Kind == isb.Data {
			dataMessages = append(dataMessages, m)
		}
	}

	// Metrics for reading data, we use ReadDataMessagesCount for calculating processing rate as well for a vertex
	metrics.ReadDataMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(dataMessages)))
	metrics.ReadMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readMessages)))

	//// fetch watermark if available
	//// TODO: make it async (concurrent and wait later)
	//// let's track only the first element's watermark. This is important because we reassign the watermark we fetch
	//// to all the elements in the batch. If we were to assign last element's watermark, we will wrongly mark on-time data as late.
	//// we fetch the watermark for the partition from which we read the message.
	// TODO(stream): enable to check for watermark here
	//processorWM := isdf.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, isdf.fromBufferPartition.GetPartitionIdx())

	// Step 2: UDF processing
	// This involves sending the read messages to the UDF and getting the results
	// To keep things asynchronous, we do not do a blocking wait on the responses from the UDF.
	// For facilitating this, we use a bi-directional grpc stream connection, on which we keep sending the requests
	// and then wait for the responses concurrently.
	// The responses are then sent to the writer for writing to the toBuffers.
	// writeChan is the channel on which the responses from the UDF are sent, and then this is consumed by
	// the writer
	// These responses are then sent to the writer for writing to the toBuffers.
	// This channel is closed when the UDF processing is done, to indicate that no further processing is required.
	// TODO(stream): on an error, we should close the writeChan, so that the writer can stop processing.
	// TODO(stream): should we keep this buffered so that on shutdown we can drain whatever is completed,
	// either as ack/no ack, also to check the responsibility for close
	writeChan := make(chan *types.ResponseFlatmap)

	// send the input messages for processing
	// The error channel is used to signal any errors that might have occurred during the UDF processing.
	// TODO(stream): add the error handler for graceful handling
	udfErrorCh := isdf.flatmapUDF.ApplyMap(ctx, dataMessages, writeChan)

	// TODO(stream): Publish the watermark for these writers
	ackChan := isdf.invokeWriter(ctx, writeChan, udfErrorCh)

	done := isdf.invokeAck(ctx, ackChan)

	<-done

	// when we apply udf, we don't handle partial errors (it's either non or all, non will return early),
	// so we should be able to ack all the readOffsets including data messages and control messages
	//err = isdf.ackFromBuffer(ctx, readOffsets)
	//// implicit return for posterity :-)
	//if err != nil {
	//	isdf.opts.logger.Errorw("Failed to ack from buffer", zap.Error(err))
	//	metrics.AckMessageError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))
	//	return
	//}
	//metrics.AckMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))

	// ProcessingTimes of the entire forwardAChunk
	//metrics.ForwardAChunkProcessingTime.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Observe(float64(time.Since(start).Microseconds()))
	isdf.opts.logger.Debugw("concurrent applyUDF completed", zap.Int("concurrency", isdf.opts.udfConcurrency), zap.Duration("took", time.Since(start)))
}

//
//// invokeUDF reads requests from the supplied PBQ, invokes the UDF to gets the response and writes the response to the
//// main channel.
//func (isdf *InterStepDataForward) invokeUDF(ctx context.Context, dataMessages []*isb.ReadMessage) (<-chan *types.ResponseFlatmap, <-chan error) {
//
//}

func (isdf *InterStepDataForward) ackRoutine(ctx context.Context, ackMsgChan <-chan *isb.ReadMessage, wg *sync.WaitGroup) {
	defer wg.Done()
forwardLoop:
	for {
		select {
		case <-ctx.Done():
			break forwardLoop

		case response, ok := <-ackMsgChan:
			if !ok {
				break forwardLoop
			}
			ackMessages := []isb.Offset{response.ReadOffset}
			if err := isdf.ackFromBuffer(ctx, ackMessages); err != nil {
				isdf.opts.logger.Error("MYDEBUG: ERROR IN ACK ", zap.Error(err))
				// TODO(stream): we have retried in the ackFromBuffer, should we trigger
				// shutdown here then?
			}
			//isdf.opts.logger.Info("MYDEBUG: NEW GOT TO ACK THIS ", response.ReadOffset)
		}
	}
}
func (isdf *InterStepDataForward) invokeAck(ctx context.Context, ackMsgChan <-chan *isb.ReadMessage) (doneChan chan struct{}) {
	logger := isdf.opts.logger
	logger.Info("MYDEBUG: NO WG ACK ROUTINE ", isdf.opts.readBatchSize)
	doneChan = make(chan struct{})
	go func() {
		defer close(doneChan)
		group := sync.WaitGroup{}
		for i := 0; i < int(isdf.opts.readBatchSize); i++ {
			group.Add(1)
			go isdf.ackRoutine(ctx, ackMsgChan, &group)
		}
		group.Wait()
	}()
	return doneChan
}

func (isdf *InterStepDataForward) writeRoutine(ctx context.Context, writeMessageCh <-chan *types.ResponseFlatmap, errCh <-chan error, ackChan chan<- *isb.ReadMessage, wg *sync.WaitGroup) {
	defer wg.Done()
outerLoop:
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				isdf.opts.logger.Infow("Context is canceled, stopping the writer", zap.Error(err))
				return
			}
			if err != nil {
				isdf.opts.logger.Error("MYDEBUG: NEW ERROR IN ERROR CH WRITE", zap.Error(err))
				// TODO(stream): got a error from the UDF, time to start the shutdown then or retry
				//isdf.opts.logger.Panic("Got an error while invoking ApplyMap", zap.Error(err))
			}
		case response, ok := <-writeMessageCh:
			if !ok {
				break outerLoop
			}
			//isdf.opts.logger.Info("MYDEBUG: NEW SENDING TO ACK ", response.ParentMessage.ReadOffset, " ", response.Uid)
			var messageToStep = make(map[string][]isb.Message)
			for toVertex := range isdf.toBuffers {
				// over allocating to have a predictable pattern
				messageToStep[toVertex] = make([]isb.Message, len(isdf.toBuffers[toVertex]))
			}
			writeMessage := response.RespMessage
			writeMessages := []*isb.WriteMessage{writeMessage}
			if err := isdf.forwardToBuffers(ctx, &writeMessages, response.ParentMessage, messageToStep); err != nil {
				//	// TODO(stream): mark as no ack directly or retry
				isdf.opts.logger.Error("MYDEBUG: NEW ERROR IN WRITE", zap.Error(err))
			}

			metrics.UDFWriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(1))
			ackChan <- response.ParentMessage
		}
	}

}

func (isdf *InterStepDataForward) invokeWriter(ctx context.Context, writeMessageCh <-chan *types.ResponseFlatmap, errCh <-chan error) <-chan *isb.ReadMessage {
	ackChan := make(chan *isb.ReadMessage)
	go func() {
		defer close(ackChan)
		group := sync.WaitGroup{}
		for i := 0; i < int(isdf.opts.readBatchSize); i++ {
			group.Add(1)
			go isdf.writeRoutine(ctx, writeMessageCh, errCh, ackChan, &group)
		}
		group.Wait()
	}()
	return ackChan
}

func (isdf *InterStepDataForward) forwardToBuffers(ctx context.Context, writeMessages *[]*isb.WriteMessage, readMessage *isb.ReadMessage, messageToStep map[string][]isb.Message) error {
	if len(*writeMessages) == 0 {
		return nil
	}
	for _, message := range *writeMessages {
		if err := isdf.whereToStep(message, messageToStep, readMessage); err != nil {
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
			return err
		}
	}

	// forward the messages to the edge buffer (could be multiple edges)
	_, err := isdf.writeToBuffers(ctx, messageToStep)
	if err != nil {
		isdf.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
		return err
	}
	return nil
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
	ctx context.Context, messageToStep map[string][]isb.Message,
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
func (isdf *InterStepDataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, msg isb.Message) (writeOffsets []isb.Offset, err error) {
	var (
		//totalCount int
		writeCount int
		writeBytes float64
	)
	//totalCount = len(messages)
	//writeOffsets = make([]isb.Offset, 0, totalCount)

	for {
		// EXTRA
		//var _writeOffsets []isb.Offset = nil
		//var errs []error = nil
		_writeOffsets, errs := toBufferPartition.Write(ctx, []isb.Message{msg})
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages isb.Message
		needRetry := false
		//for idx, msg := range messages {
		// EXTRA
		//if err != nil {
		if err = errs[0]; err != nil {
			// ATM there are no user-defined errors during write, all are InternalErrors.
			// Non retryable error, drop the message. Non retryable errors are only returned
			// when the buffer is full and the user has set the buffer full strategy to
			// DiscardLatest or when the message is duplicate.
			if errors.As(err, &isb.NonRetryableBufferWriteErr{}) {
				metrics.DropMessagesCount.With(map[string]string{
					metrics.LabelVertex:             isdf.vertexName,
					metrics.LabelPipeline:           isdf.pipelineName,
					metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
					metrics.LabelPartitionName:      toBufferPartition.GetName(),
					metrics.LabelReason:             err.Error(),
				}).Inc()

				metrics.DropBytesCount.With(map[string]string{
					metrics.LabelVertex:             isdf.vertexName,
					metrics.LabelPipeline:           isdf.pipelineName,
					metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
					metrics.LabelPartitionName:      toBufferPartition.GetName(),
					metrics.LabelReason:             err.Error(),
				}).Add(float64(len(msg.Payload)))

				isdf.opts.logger.Infow("Dropped message", zap.String("reason", err.Error()), zap.String("partition", toBufferPartition.GetName()), zap.String("vertex", isdf.vertexName), zap.String("pipeline", isdf.pipelineName))
			} else {
				needRetry = true
				// we retry only failed messages
				failedMessages = msg
				metrics.WriteMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Inc()
				// a shutdown can break the blocking loop caused due to InternalErr
				if ok, _ := isdf.IsShuttingDown(); ok {
					metrics.PlatformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Inc()
					return writeOffsets, fmt.Errorf("writeToBuffer failed, Stop called while stuck on an internal error with failed messages: %v", errs)
				}
			}
		} else {
			writeCount++
			writeBytes += float64(len(msg.Payload))
			// we support write offsets only for jetstream
			if _writeOffsets != nil {
				writeOffsets = _writeOffsets
			}
		}
		//}

		if needRetry {
			isdf.opts.logger.Errorw("Retrying failed messages",
				zap.Any("errors", errorArrayToMap(errs)),
				zap.String(metrics.LabelPipeline, isdf.pipelineName),
				zap.String(metrics.LabelVertex, isdf.vertexName),
				zap.String(metrics.LabelPartitionName, toBufferPartition.GetName()),
			)
			// set messages to failed for the retry
			msg = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
		} else {
			break
		}
	}

	metrics.WriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(writeCount))
	metrics.WriteBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(writeBytes)
	return writeOffsets, nil
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *InterStepDataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors
	to, err := isdf.FSD.WhereTo(writeMessage.Keys, writeMessage.Tags, writeMessage.ID)
	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := isdf.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			metrics.PlatformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Inc()
			return err
		}
		return err
	}

	for _, t := range to {
		if _, ok := messageToStep[t.ToVertexName]; !ok {
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName)}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = writeMessage.Message
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

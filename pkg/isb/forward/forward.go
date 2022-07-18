/*
Package forward does the Read (fromBuffer) -> Process (UDF) -> Forward (toBuffers) -> Ack (fromBuffer) loop.
*/
package forward

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	udfapplier "github.com/numaproj/numaflow/pkg/udf/applier"
)

// InterStepDataForward forwards the data from previous step to the current step via inter-step buffer.
type InterStepDataForward struct {
	// I have my reasons for overriding the default principle https://github.com/golang/go/issues/22602
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn         context.CancelFunc
	fromBuffer       isb.BufferReader
	toBuffers        map[string]isb.BufferWriter
	FSD              ToWhichStepDecider
	UDF              udfapplier.Applier
	fetchWatermark   fetch.Fetcher
	publishWatermark map[string]publish.Publisher
	opts             options
	vertexName       string
	pipelineName     string
	Shutdown
}

// NewInterStepDataForward creates an inter-step forwarder.
func NewInterStepDataForward(vertex *dfv1.Vertex,
	fromStep isb.BufferReader,
	toSteps map[string]isb.BufferWriter,
	fsd ToWhichStepDecider,
	applyUDF udfapplier.Applier,
	fetchWatermark fetch.Fetcher,
	publishWatermark map[string]publish.Publisher,
	opts ...Option) (*InterStepDataForward, error) {

	options := &options{
		retryInterval:  time.Millisecond,
		readBatchSize:  1,
		udfConcurrency: 1,
		logger:         logging.NewLogger(),
	}
	for _, o := range opts {
		if err := o(options); err != nil {
			return nil, err
		}
	}
	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	isdf := InterStepDataForward{
		ctx:              ctx,
		cancelFn:         cancel,
		fromBuffer:       fromStep,
		toBuffers:        toSteps,
		FSD:              fsd,
		UDF:              applyUDF,
		fetchWatermark:   fetchWatermark,
		publishWatermark: publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:   vertex.Spec.Name,
		pipelineName: vertex.Spec.PipelineName,
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}

	// Add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, options.logger)

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
				// shutdown the fromBuffer should be empty.
			}
			// keep doing what you are good at
			isdf.forwardAChunk(isdf.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// Clean up resources for buffer reader and all the writers if any.
		if err := isdf.fromBuffer.Close(); err != nil {
			log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed buffer reader", zap.String("bufferFrom", isdf.fromBuffer.GetName()))
		}
		for _, v := range isdf.toBuffers {
			if err := v.Close(); err != nil {
				log.Errorw("Failed to close buffer writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", v.GetName()))
			} else {
				log.Infow("Closed buffer writer", zap.String("bufferTo", v.GetName()))
			}
		}

		// stop watermark publisher if watermarking is enabled
		if isdf.publishWatermark != nil {
			for _, publisher := range isdf.publishWatermark {
				publisher.StopPublisher()
			}
		}
		close(stopped)
	}()

	return stopped
}

// readWriteMessagePair represents a read message and its processed (via UDF) write message.
type readWriteMessagePair struct {
	readMessage   *isb.ReadMessage
	writeMessages []*isb.Message
	udfError      error
}

// forwardAChunk forwards a chunk of message from the fromBuffer to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to user code UDFs, WhereTo, etc.
func (isdf *InterStepDataForward) forwardAChunk(ctx context.Context) {
	start := time.Now()
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := isdf.fromBuffer.Read(ctx, isdf.opts.readBatchSize)
	if err != nil {
		isdf.opts.logger.Warnw("failed to read fromBuffer", zap.Error(err))
		readMessagesError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Inc()
	}
	readMessagesCount.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Add(float64(len(readMessages)))

	// process only if we have any read messages. There is a natural looping here if there is an internal error while
	// reading, and we are not able to proceed.
	if len(readMessages) == 0 {
		return
	}

	// fetch watermark if available
	// TODO: make it async (concurrent and wait later)
	var processorWM processor.Watermark
	if isdf.fetchWatermark != nil {
		// let's track only the last element's watermark
		processorWM = isdf.fetchWatermark.GetWatermark(readMessages[len(readMessages)-1].ReadOffset)
	}

	// create space for writeMessages specific to each step as we could forward to all the steps too.
	var messageToStep = make(map[string][]isb.Message)
	var toBuffers string
	for step := range isdf.toBuffers {
		// over allocating to have a predictable pattern
		messageToStep[step] = make([]isb.Message, 0, len(readMessages))
		toBuffers += step
	}

	// udf concurrent processing request channel
	udfCh := make(chan *readWriteMessagePair)
	// udfResults stores the results after UDF processing for all read messages. It indexes
	// a read message to the corresponding write message
	udfResults := make([]readWriteMessagePair, len(readMessages))
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
	// send UDF processing work to the channel
	for idx, readMessage := range readMessages {
		udfResults[idx].readMessage = readMessage
		udfCh <- &udfResults[idx]
	}
	// let the go routines know that there is no more work
	close(udfCh)
	// wait till the processing is done. this will not be an infinite wait because the UDF processing will exit if
	// context.Done() is closed.
	wg.Wait()
	isdf.opts.logger.Debugw("concurrent applyUDF completed", zap.Int("concurrency", isdf.opts.udfConcurrency), zap.Duration("took", time.Since(concurrentUDFProcessingStart)))
	concurrentUDFProcessingTime.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Observe(float64(time.Since(concurrentUDFProcessingStart).Microseconds()))

	// Now that we know the UDF processing is done, let's figure out which vertex to send the results to.
	// Update the toBuffer(s) with writeMessages.
	for _, m := range udfResults {
		// look for errors in udf processing, if we see even 1 error let's return. handling partial retrying is not worth ATM.
		if m.udfError != nil {
			udfError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Inc()
			isdf.opts.logger.Errorw("failed to applyUDF", zap.Error(err))
			return
		}
		// update toBuffers
		for _, message := range m.writeMessages {
			if err := isdf.whereToStep(message, messageToStep, m.readMessage); err != nil {
				isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
				return
			}
		}
	}
	// forward the message to the edge buffer (could be multiple edges)
	writeOffsets, err := isdf.writeToBuffers(ctx, messageToStep)
	if err != nil {
		isdf.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
		return
	}

	// forward the highest watermark to all the edges to avoid idle edge problem
	if isdf.publishWatermark != nil {
		// TODO: sort and get the highest value
		for edgeName, offsets := range writeOffsets {
			if len(offsets) > 0 {
				isdf.publishWatermark[edgeName].PublishWatermark(processorWM, offsets[len(offsets)-1])
			}
		}
	}

	// let us ack the only if we have successfully forwarded all the messages.
	// we need the readOffsets to acknowledge later
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range udfResults {
		readOffsets[idx] = m.readMessage.ReadOffset
	}
	err = isdf.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		isdf.opts.logger.Errorw("failed to ack from buffer", zap.Error(err))
		ackMessageError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Add(float64(len(readOffsets)))
		return
	}
	ackMessagesCount.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Add(float64(len(readOffsets)))

	// ProcessingTimes of the entire forwardAChunk
	forwardAChunkProcessingTime.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "from": isdf.fromBuffer.GetName(), "to": toBuffers}).Observe(float64(time.Since(start).Microseconds()))
}

// ackFromBuffer acknowledges an array of offsets back to fromBuffer and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) (err error) {
	for {
		errs := isdf.fromBuffer.Ack(ctx, offsets)
		summarizedErr := errorArrayToMap(errs)
		if len(summarizedErr) > 0 {
			isdf.opts.logger.Errorw("failed to ack from buffer", zap.Any("errors", summarizedErr))
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
			if ok, _ := isdf.IsShuttingDown(); ok {
				err := fmt.Errorf("ackFromBuffer, Stop called while stuck on an internal error, %v", summarizedErr)
				return err
			}
			// TODO: only retry failed ones.
		} else {
			break
		}
	}
	return err
}

// writeToBuffers is a blocking call until all the messages have be forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (isdf *InterStepDataForward) writeToBuffers(ctx context.Context, messageToStep map[string][]isb.Message) (writeOffsetsEdge map[string][]isb.Offset, err error) {
	writeOffsetsEdge = make(map[string][]isb.Offset, len(messageToStep))
	for key, toBuffer := range isdf.toBuffers {
		writeOffsetsEdge[key], err = isdf.writeToBuffer(ctx, toBuffer, messageToStep[key])
		if err != nil {
			return writeOffsetsEdge, err
		}
	}

	return writeOffsetsEdge, nil
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) writeToBuffer(ctx context.Context, toBuffer isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
	writeOffsets = make([]isb.Offset, 0, len(messages))
retry:
	needRetry := false
	for {
		_writeOffsets, errs := toBuffer.Write(ctx, messages)
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages []isb.Message
		for idx := range messages {
			// use the messages index to index error, there is a 1:1 mapping
			err := errs[idx]
			// ATM there are no user defined errors during write, all are InternalErrors, and they require retry.
			if err != nil {
				needRetry = true
				// we retry only failed messages
				failedMessages = append(failedMessages, messages[idx])
				writeMessagesError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": toBuffer.GetName()}).Inc()
				// a shutdown can break the blocking loop caused due to InternalErr
				if ok, _ := isdf.IsShuttingDown(); ok {
					err := fmt.Errorf("writeToBuffer failed, Stop called while stuck on an internal error with failed messages:%d, %v", len(failedMessages), errs)
					platformError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName}).Inc()
					return writeOffsets, err
				}
			} else {
				// we support write offsets only for jetstream
				if _writeOffsets != nil {
					writeOffsets = append(writeOffsets, _writeOffsets[idx])
				}
			}
		}
		// set messages to failed for the retry
		if needRetry {
			isdf.opts.logger.Errorw("Retrying failed msgs", zap.Any("errors", errorArrayToMap(errs)))
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
			goto retry
		} else {
			break
		}
	}

	writeMessagesCount.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": toBuffer.GetName()}).Add(float64(len(messages)))

	return writeOffsets, nil
}

// concurrentApplyUDF applies the UDF based on the request from the channel
func (isdf *InterStepDataForward) concurrentApplyUDF(ctx context.Context, readMessagePair <-chan *readWriteMessagePair) {
	for message := range readMessagePair {
		start := time.Now()
		udfReadMessagesCount.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Inc()
		writeMessages, err := isdf.applyUDF(ctx, message.readMessage)
		udfWriteMessagesCount.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Add(float64(len(writeMessages)))
		message.writeMessages = append(message.writeMessages, writeMessages...)
		message.udfError = err
		udfProcessingTime.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName, "buffer": isdf.fromBuffer.GetName()}).Observe(float64(time.Since(start).Microseconds()))
	}
}

// applyUDF applies the UDF and will block if there is any InternalErr. On the other hand, if this is a UserError
// the skip flag is set. ShutDown flag will only if there is an InternalErr and ForceStop has been invoked.
// The UserError retry will be done on the ApplyUDF.
func (isdf *InterStepDataForward) applyUDF(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
	for {
		writeMessages, err := isdf.UDF.Apply(ctx, readMessage)
		if err != nil {
			isdf.opts.logger.Errorw("UDF.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform we should not lose or corrupt data.
			// this does not mean we should prohibit this from a shutdown.
			if ok, _ := isdf.IsShuttingDown(); ok {
				isdf.opts.logger.Errorw("UDF.Apply, Stop called while stuck on an internal error", zap.Error(err))
				platformError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName}).Inc()
				return nil, err
			}
			continue
		} else {
			// if we do not get a time from UDF, we set it to the time from (N-1)th vertex
			for _, m := range writeMessages {
				if m.EventTime.IsZero() {
					m.EventTime = readMessage.EventTime
				}
			}
			return writeMessages, nil
		}
	}
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *InterStepDataForward) whereToStep(writeMessage *isb.Message, messageToStep map[string][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors

	to, err := isdf.FSD.WhereTo(writeMessage.Key)

	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBuffer.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := isdf.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			platformError.With(map[string]string{"vertex": isdf.vertexName, "pipeline": isdf.pipelineName}).Inc()
			return err
		}
		return err
	}

	switch {
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyAll):
		for toStep := range isdf.toBuffers {
			// update all the destination
			messageToStep[toStep] = append(messageToStep[toStep], *writeMessage)

		}
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyDrop):
	default:
		for _, t := range to {
			if _, ok := messageToStep[t]; !ok {
				isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBuffer.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t)}))
			}
			messageToStep[t] = append(messageToStep[t], *writeMessage)
		}
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

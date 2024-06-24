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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
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
	cancelFn             context.CancelFunc
	reader               sourcer.SourceReader          // reader reads data from source.
	toBuffers            map[string][]isb.BufferWriter // toBuffers store the toVertex name to its owned buffers mapping.
	toWhichStepDecider   forwarder.ToWhichStepDecider
	wmFetcher            fetch.SourceFetcher
	toVertexWMStores     map[string]store.WatermarkStore
	toVertexWMPublishers map[string]map[int32]publish.Publisher // toVertexWMPublishers stores the toVertex to publisher mapping.
	srcWMPublisher       publish.SourcePublisher                // srcWMPublisher is used to publish source watermark.
	opts                 options
	vertexName           string
	pipelineName         string
	vertexReplica        int32
	watermarkConfig      dfv1.Watermark
	idleManager          wmb.IdleManager // idleManager manages the idle watermark status.
	srcIdleHandler       *idlehandler.SourceIdleHandler
	Shutdown
}

// NewDataForward creates a source data forwarder
func NewDataForward(
	vertexInstance *dfv1.VertexInstance,
	reader sourcer.SourceReader,
	toSteps map[string][]isb.BufferWriter,
	toWhichStepDecider forwarder.ToWhichStepDecider,
	fetchWatermark fetch.SourceFetcher,
	srcWMPublisher publish.SourcePublisher,
	toVertexWmStores map[string]store.WatermarkStore,
	idleManager wmb.IdleManager,
	opts ...Option) (*DataForward, error) {
	dOpts := defaultOptions()
	for _, o := range opts {
		if err := o(dOpts); err != nil {
			return nil, err
		}
	}

	var toVertexWMPublishers = make(map[string]map[int32]publish.Publisher)
	for k := range toVertexWmStores {
		toVertexWMPublishers[k] = make(map[int32]publish.Publisher)
	}

	// create a source idle handler
	srcIdleHandler := idlehandler.NewSourceIdleHandler(&vertexInstance.Vertex.Spec.Watermark, fetchWatermark, srcWMPublisher)

	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	var isdf = DataForward{
		ctx:                  ctx,
		cancelFn:             cancel,
		reader:               reader,
		toBuffers:            toSteps,
		toWhichStepDecider:   toWhichStepDecider,
		wmFetcher:            fetchWatermark,
		toVertexWMStores:     toVertexWmStores,
		toVertexWMPublishers: toVertexWMPublishers,
		srcWMPublisher:       srcWMPublisher,
		vertexName:           vertexInstance.Vertex.Spec.Name,
		pipelineName:         vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:        vertexInstance.Replica,
		watermarkConfig:      vertexInstance.Vertex.Spec.Watermark,
		idleManager:          idleManager,
		srcIdleHandler:       srcIdleHandler,
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *dOpts,
	}
	// add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, dOpts.logger)
	return &isdf, nil
}

// Start starts reading from source and forwards to the next buffers. Call `Stop` to stop.
func (df *DataForward) Start() <-chan struct{} {
	log := logging.FromContext(df.ctx)
	stopped := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Info("Starting forwarder...")
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
				// shutdown the reader should be empty.
			}
			// keep doing what you are good at
			df.forwardAChunk(df.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// clean up resources for source reader and all the writers if any.
		if err := df.reader.Close(); err != nil {
			log.Errorw("Failed to close source reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed source reader", zap.String("sourceFrom", df.reader.GetName()))
		}
		for _, buffer := range df.toBuffers {
			for _, partition := range buffer {
				if err := partition.Close(); err != nil {
					log.Errorw("Failed to close partition writer, shutdown anyways...",
						zap.Error(err),
						zap.String("bufferTo", partition.GetName()),
					)
				} else {
					log.Infow("Closed partition writer", zap.String("bufferTo", partition.GetName()))
				}
			}
		}

		// the publisher was created by the forwarder, so it should be closed by the forwarder.
		for _, toVertexPublishers := range df.toVertexWMPublishers {
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

// forwardAChunk forwards a chunk of message from the reader to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but do not include errors due to user code transformer, WhereTo, etc.
func (df *DataForward) forwardAChunk(ctx context.Context) {
	start := time.Now()
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during the restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := df.reader.Read(ctx, df.opts.readBatchSize)
	if err != nil {
		df.opts.logger.Warnw("failed to read from source", zap.Error(err))
		metrics.ReadMessagesError.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Inc()
	}

	// if there are no read messages, we return early.
	if len(readMessages) == 0 {
		// not idling, so nothing much to do
		if !df.srcIdleHandler.IsSourceIdling() {
			return
		}

		// if the source is idling, we will publish idle watermark to the source and all the toBuffers
		// we will not publish idle watermark if the source is not idling.
		// publish idle watermark for the source
		df.srcIdleHandler.PublishSourceIdleWatermark(df.reader.Partitions(df.ctx))

		// if we have published idle watermark to source, we need to publish idle watermark to all the toBuffers
		// it might not get the latest watermark because of publishing delay, but we will get in the subsequent
		// iterations.

		// publish idle watermark for all the toBuffers
		fetchedWm := df.wmFetcher.ComputeWatermark()
		for toVertexName, toVertexBuffers := range df.toBuffers {
			for index := range toVertexBuffers {
				// publish idle watermark to all the source partitions owned by this reader.
				// it is 1:1 for many (HTTP, tickgen, etc.) but for e.g., for Kafka it is 1:N and the list of partitions in the N could keep changing.
				for _, sp := range df.reader.Partitions(df.ctx) {
					if vertexPublishers, ok := df.toVertexWMPublishers[toVertexName]; ok {
						var publisher, ok = vertexPublishers[sp]
						if !ok {
							publisher = df.createToVertexWatermarkPublisher(toVertexName, sp)
							vertexPublishers[sp] = publisher
						}
						idlehandler.PublishIdleWatermark(ctx, wmb.PARTITION_0, df.toBuffers[toVertexName][index], publisher,
							df.idleManager, df.opts.logger, df.vertexName, df.pipelineName, dfv1.VertexTypeSource, df.vertexReplica, fetchedWm)
					}
				}
			}
		}

		// len(readMessages) == 0, so we do not have anything more to do
		return
	}

	// reset the idle handler because we have read messages
	df.srcIdleHandler.Reset()
	metrics.ReadDataMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.reader.GetName()},
	).Add(float64(len(readMessages)))

	metrics.ReadMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.reader.GetName(),
	}).Add(float64(len(readMessages)))

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
	for toVertex := range df.toBuffers {
		// over allocating to have a predictable pattern
		messageToStep[toVertex] = make([][]isb.Message, len(df.toBuffers[toVertex]))
	}

	readWriteMessagePairs := make([]isb.ReadWriteMessagePair, len(readMessages))

	// If a user-defined transformer exists, apply it
	if df.opts.transformer != nil {
		// user-defined transformer concurrent processing request channel
		transformerCh := make(chan *isb.ReadWriteMessagePair)

		// create a pool of Transformer Processors
		var wg sync.WaitGroup
		for i := 0; i < df.opts.transformerConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				df.concurrentApplyTransformer(ctx, transformerCh)
			}()
		}
		concurrentTransformerProcessingStart := time.Now()
		for idx, m := range readMessages {
			metrics.ReadBytesCount.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				metrics.LabelPartitionName:      df.reader.GetName(),
			}).Add(float64(len(m.Payload)))

			// assign watermark to the message
			m.Watermark = time.Time(processorWM)
			readWriteMessagePairs[idx].ReadMessage = m
			// send transformer processing work to the channel. Thus, the results of the transformer
			// application on a read message will be stored as the corresponding writeMessage in readWriteMessagePairs
			transformerCh <- &readWriteMessagePairs[idx]
		}
		// let the go routines know that there is no more work
		close(transformerCh)
		// wait till the processing is done. this will not be an infinite wait because the transformer processing will exit if
		// context.Done() is closed.
		wg.Wait()
		df.opts.logger.Debugw("concurrent applyTransformer completed",
			zap.Int("concurrency", df.opts.transformerConcurrency),
			zap.Duration("took", time.Since(concurrentTransformerProcessingStart)),
		)

		metrics.SourceTransformerConcurrentProcessingTime.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Observe(float64(time.Since(concurrentTransformerProcessingStart).Microseconds()))
	} else {
		for idx, m := range readMessages {
			metrics.ReadBytesCount.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				metrics.LabelPartitionName:      df.reader.GetName(),
			}).Add(float64(len(m.Payload)))

			// assign watermark to the message
			m.Watermark = time.Time(processorWM)
			readWriteMessagePairs[idx].ReadMessage = m
			// if no user-defined transformer exists, then the messages to write will be identical to the message read from source
			// thus, the unmodified read message will be stored as the corresponding writeMessage in readWriteMessagePairs
			readWriteMessagePairs[idx].WriteMessages = []*isb.WriteMessage{{Message: m.Message}}
		}
	}

	// publish source watermark and assign IsLate attribute based on new event time.
	var writeMessages []*isb.WriteMessage
	var transformedReadMessages []*isb.ReadMessage
	latestEtMap := make(map[int32]int64)

	for _, m := range readWriteMessagePairs {
		writeMessages = append(writeMessages, m.WriteMessages...)
		for _, message := range m.WriteMessages {
			message.Headers = m.ReadMessage.Headers
			// we convert each writeMessage to isb.ReadMessage by providing its parent ReadMessage's ReadOffset.
			// since we use message event time instead of the watermark to determine and publish source watermarks,
			// time.UnixMilli(-1) is assigned to the message watermark. transformedReadMessages are immediately
			// used below for publishing source watermarks.
			if latestEt, ok := latestEtMap[m.ReadMessage.ReadOffset.PartitionIdx()]; !ok || message.EventTime.UnixNano() < latestEt {
				latestEtMap[m.ReadMessage.ReadOffset.PartitionIdx()] = message.EventTime.UnixNano()
			}
			transformedReadMessages = append(transformedReadMessages, message.ToReadMessage(m.ReadMessage.ReadOffset, time.UnixMilli(-1)))
		}
	}

	// publish source watermark
	df.srcWMPublisher.PublishSourceWatermarks(transformedReadMessages)
	// update the watermark configs for lastTimestampSrcWMUpdated, lastFetchedSrcWatermark and lastTimestampIdleWMFound.
	// fetch the source watermark again, we might not get the latest watermark because of publishing delay,
	// but ideally we should use the latest to determine the IsLate attribute.
	processorWM = df.wmFetcher.ComputeWatermark()
	// assign isLate
	for _, m := range writeMessages {
		if processorWM.After(m.EventTime) { // Set late data at source level
			m.IsLate = true
		}
	}

	var sourcePartitionsIndices = make(map[int32]bool)
	// let's figure out which vertex to send the results to.
	// update the toBuffer(s) with writeMessages.
	for _, m := range readWriteMessagePairs {
		// Look for errors in transformer processing if we see even 1 error we return.
		// Handling partial retrying is not worth ATM.
		if m.Err != nil {
			metrics.SourceTransformerError.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				metrics.LabelPartitionName:      df.reader.GetName(),
			}).Inc()

			df.opts.logger.Errorw("failed to apply source transformer", zap.Error(m.Err))
			return
		}
		// for each message, we will determine where to send the message.
		for _, message := range m.WriteMessages {
			if err = df.whereToStep(message, messageToStep); err != nil {
				df.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
				return
			}
		}
		// get the list of source partitions for which we have read messages, we will use this to publish watermarks to toVertices
		sourcePartitionsIndices[m.ReadMessage.ReadOffset.PartitionIdx()] = true
	}

	// forward the messages to the edge buffer (could be multiple edges)
	writeOffsets, err = df.writeToBuffers(ctx, messageToStep)
	if err != nil {
		df.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
		return
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// It's used to determine which buffers should receive an idle watermark.
	// It is created as a slice because it tracks per partition activity info.
	// when there are no messages read from the source, we will publish idle watermark to all the toBuffers.
	var activeWatermarkBuffers = make(map[string][]bool)
	// forward the highest watermark to all the edges to avoid idle edge problem
	// TODO: sort and get the highest value
	for toVertexName, toVertexBufferOffsets := range writeOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(toVertexBufferOffsets))
		if vertexPublishers, ok := df.toVertexWMPublishers[toVertexName]; ok {
			for index, offsets := range toVertexBufferOffsets {
				if len(offsets) > 0 {
					// publish watermark to all the source partitions
					// create a publisher if it doesn't exist, we don't want to publish to all the partitions
					for sp := range sourcePartitionsIndices {
						var publisher, ok = vertexPublishers[sp]
						if !ok {
							publisher = df.createToVertexWatermarkPublisher(toVertexName, sp)
							vertexPublishers[sp] = publisher
						}

						publisher.PublishWatermark(processorWM, offsets[len(offsets)-1], int32(index))
						activeWatermarkBuffers[toVertexName][index] = true
						// update the watermark configs for lastTimestampSrcWMUpdated, lastFetchedSrcWatermark and lastTimestampIdleWMFound.
						// reset because the toBuffer partition is no longer idling
						df.idleManager.MarkActive(wmb.PARTITION_0, df.toBuffers[toVertexName][index].GetName())
					}
				}
				// This (len(offsets) == 0) happens at conditional forwarding, there's no data written to the buffer
			}
		}
	}

	// condition "len(activeWatermarkBuffers) < len(df.toVertexWMPublishers)":
	// send idle watermark only if we have idled out buffers
	for toVertexName := range df.toVertexWMPublishers {
		for index, activePartition := range activeWatermarkBuffers[toVertexName] {
			if !activePartition {
				// same as read len==0 because there's no event published to the buffer
				if vertexPublishers, ok := df.toVertexWMPublishers[toVertexName]; ok {
					for sp := range sourcePartitionsIndices {
						var publisher, ok = vertexPublishers[sp]
						if !ok {
							publisher = df.createToVertexWatermarkPublisher(toVertexName, sp)
							vertexPublishers[sp] = publisher
						}
						idlehandler.PublishIdleWatermark(ctx, wmb.PARTITION_0, df.toBuffers[toVertexName][index], publisher,
							df.idleManager, df.opts.logger, df.vertexName, df.pipelineName, dfv1.VertexTypeSource, df.vertexReplica, processorWM)
					}
				}
			}
		}
	}

	// when we apply transformer, we don't handle partial errors (it's either non or all, non will return early),
	// so we should be able to ack all the readOffsets including data messages and control messages
	err = df.ackFromSource(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		df.opts.logger.Errorw("failed to ack from source", zap.Error(err))
		metrics.AckMessageError.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Add(float64(len(readOffsets)))

		return
	}
	metrics.AckMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.reader.GetName(),
	}).Add(float64(len(readOffsets)))

	if df.opts.cbPublisher != nil {
		if err := df.opts.cbPublisher.NonSinkVertexCallback(ctx, readWriteMessagePairs); err != nil {
			df.opts.logger.Errorw("failed to publish callback", zap.Error(err))
		}
	}

	// ProcessingTimes of the entire forwardAChunk
	metrics.ForwardAChunkProcessingTime.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
	}).Observe(float64(time.Since(start).Microseconds()))
}

func (df *DataForward) ackFromSource(ctx context.Context, offsets []isb.Offset) error {
	// for all the sources, we either ack all offsets or none.
	// when a batch ack fails, the source Ack() function populate the error array with the same error;
	// hence we can just return the first error.
	return df.reader.Ack(ctx, offsets)[0]
}

// writeToBuffers is a blocking call until all the messages have been forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (df *DataForward) writeToBuffers(
	ctx context.Context, messageToStep map[string][][]isb.Message,
) (writeOffsets map[string][][]isb.Offset, err error) {
	// messageToStep contains all the to buffers, so the messages could be empty (conditional forwarding).
	// So writeOffsets also contains all the to buffers, but the returned offsets might be empty.
	writeOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexMessages := range messageToStep {
		writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexMessages))
	}
	for toVertexName, toVertexBuffer := range df.toBuffers {
		for index, partition := range toVertexBuffer {
			writeOffsets[toVertexName][index], err = df.writeToBuffer(ctx, partition, messageToStep[toVertexName][index])
			if err != nil {
				return nil, err
			}
		}
	}
	return writeOffsets, nil
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (df *DataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
	var (
		totalCount int
		writeCount int
		writeBytes float64
	)
	totalCount = len(messages)
	writeOffsets = make([]isb.Offset, 0, totalCount)

	for {
		_writeOffsets, errs := toBufferPartition.Write(ctx, messages)
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages []isb.Message
		needRetry := false
		for idx, msg := range messages {
			if err = errs[idx]; err != nil {
				// ATM there are no user-defined errors during writing, all are InternalErrors.
				// Non retryable error, drop the message. Non retryable errors are only returned
				// when the buffer is full and the user has set the buffer full strategy to
				// DiscardLatest or when the message is duplicate.
				if errors.As(err, &isb.NonRetryableBufferWriteErr{}) {
					metrics.DropMessagesCount.With(map[string]string{
						metrics.LabelVertex:             df.vertexName,
						metrics.LabelPipeline:           df.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
						metrics.LabelReason:             err.Error(),
					}).Inc()

					metrics.DropBytesCount.With(map[string]string{
						metrics.LabelVertex:             df.vertexName,
						metrics.LabelPipeline:           df.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
						metrics.LabelReason:             err.Error(),
					}).Add(float64(len(msg.Payload)))

					df.opts.logger.Infow("Dropped message",
						zap.String("reason", err.Error()),
						zap.String("partition", toBufferPartition.GetName()),
						zap.String("vertex", df.vertexName), zap.String("pipeline", df.pipelineName),
					)
				} else {
					needRetry = true
					// we retry only failed messages
					failedMessages = append(failedMessages, msg)
					metrics.WriteMessagesError.With(map[string]string{
						metrics.LabelVertex:             df.vertexName,
						metrics.LabelPipeline:           df.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
					}).Inc()

					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := df.IsShuttingDown(); ok {
						metrics.PlatformError.With(map[string]string{
							metrics.LabelVertex:             df.vertexName,
							metrics.LabelPipeline:           df.pipelineName,
							metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
							metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
						}).Inc()

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
			// set messages to the failed slice for the retry
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(df.opts.retryInterval)
		} else {
			break
		}
	}

	metrics.WriteMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      toBufferPartition.GetName(),
	}).Add(float64(writeCount))

	metrics.WriteBytesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      toBufferPartition.GetName(),
	}).Add(writeBytes)

	return writeOffsets, nil
}

// concurrentApplyTransformer applies the transformer based on the request from the channel
func (df *DataForward) concurrentApplyTransformer(ctx context.Context, readMessagePair <-chan *isb.ReadWriteMessagePair) {
	for message := range readMessagePair {
		start := time.Now()
		metrics.SourceTransformerReadMessagesCount.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Inc()

		writeMessages, err := df.applyTransformer(ctx, message.ReadMessage)
		metrics.SourceTransformerWriteMessagesCount.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Add(float64(len(writeMessages)))

		message.WriteMessages = append(message.WriteMessages, writeMessages...)
		message.Err = err
		metrics.SourceTransformerProcessingTime.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.reader.GetName(),
		}).Observe(float64(time.Since(start).Microseconds()))
	}
}

// applyTransformer applies the transformer and will block if there is any InternalErr. On the other hand, if this is a UserError
// the skip flag is set. The ShutDown flag will only if there is an InternalErr and ForceStop has been invoked.
// The UserError retry will be done on the applyTransformer.
func (df *DataForward) applyTransformer(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	for {
		writeMessages, err := df.opts.transformer.ApplyTransform(ctx, readMessage)
		if err != nil {
			df.opts.logger.Errorw("Transformer.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(df.opts.retryInterval)
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform, we should not lose or corrupt data.
			// this does not mean we should prohibit this from a shutdown.
			if ok, _ := df.IsShuttingDown(); ok {
				df.opts.logger.Errorw("Transformer.Apply, Stop called while stuck on an internal error", zap.Error(err))
				metrics.PlatformError.With(map[string]string{
					metrics.LabelVertex:             df.vertexName,
					metrics.LabelPipeline:           df.pipelineName,
					metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				}).Inc()

				return nil, err
			}
			continue
		}
		return writeMessages, nil
	}
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (df *DataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][][]isb.Message) error {
	// call WhereTo and drop it on errors
	to, err := df.toWhichStepDecider.WhereTo(writeMessage.Keys, writeMessage.Tags, writeMessage.ID.String())
	if err != nil {
		df.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{
			Name:    df.reader.GetName(),
			Header:  writeMessage.Header,
			Body:    writeMessage.Body,
			Message: fmt.Sprintf("WhereTo failed, %s", err),
		}))

		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := df.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			metrics.PlatformError.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			}).Inc()

			return err
		}
		return err
	}

	for _, t := range to {
		if _, ok := messageToStep[t.ToVertexName]; !ok {
			df.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{
				Name:    df.reader.GetName(),
				Header:  writeMessage.Header,
				Body:    writeMessage.Body,
				Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName),
			}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = append(messageToStep[t.ToVertexName][t.ToVertexPartitionIdx], writeMessage.Message)
	}
	return nil
}

// createToVertexWatermarkPublisher creates a watermark publisher for the given toVertexName and partition
func (df *DataForward) createToVertexWatermarkPublisher(toVertexName string, partition int32) publish.Publisher {

	wmStore := df.toVertexWMStores[toVertexName]
	entityName := fmt.Sprintf("%s-%s-%d", df.pipelineName, df.vertexName, partition)
	processorEntity := entity.NewProcessorEntity(entityName)

	// if watermark is disabled, wmStore here is a no op store
	publisher := publish.NewPublish(df.ctx, processorEntity, wmStore, int32(len(df.toBuffers[toVertexName])))
	df.toVertexWMPublishers[toVertexName][partition] = publisher
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

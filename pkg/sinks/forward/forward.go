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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks/sinker"
	"github.com/numaproj/numaflow/pkg/sinks/udsink"
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
	sinkWriter          sinker.SinkWriter
	wmFetcher           fetch.Fetcher
	wmPublisher         publish.Publisher
	opts                options
	vertexName          string
	pipelineName        string
	vertexReplica       int32
	sinkRetryStrategy   dfv1.RetryStrategy
	// idleManager manages the idle watermark status.
	idleManager wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid.
	wmbChecker wmb.WMBChecker
	Shutdown
}

// NewDataForward creates a sink data forwarder.
func NewDataForward(
	vertexInstance *dfv1.VertexInstance,
	fromStep isb.BufferReader,
	sinkWriter sinker.SinkWriter,
	fetchWatermark fetch.Fetcher,
	publishWatermark publish.Publisher,
	idleManager wmb.IdleManager,
	opts ...Option) (*DataForward, error) {

	dOpts := DefaultOptions()
	for _, o := range opts {
		if err := o(dOpts); err != nil {
			return nil, err
		}
	}
	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	var df = DataForward{
		ctx:                 ctx,
		cancelFn:            cancel,
		fromBufferPartition: fromStep,
		sinkWriter:          sinkWriter,
		wmFetcher:           fetchWatermark,
		wmPublisher:         publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		idleManager:   idleManager,
		wmbChecker:    wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *dOpts,
	}
	// add the sink retry strategy to the forward
	if vertexInstance.Vertex.Spec.Sink != nil {
		df.sinkRetryStrategy = vertexInstance.Vertex.Spec.Sink.RetryStrategy
	}

	// Add logger from parent ctx to child context.
	df.ctx = logging.WithLogger(ctx, dOpts.logger)

	return &df, nil
}

// Start starts reading the buffer and forwards to sinker. Call `Stop` to stop.
func (df *DataForward) Start() <-chan error {
	log := logging.FromContext(df.ctx)
	stopped := make(chan error)
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
			if err := df.forwardAChunk(df.ctx); err != nil {
				log.Errorw("Failed to forward a chunk", zap.Error(err))
				stopped <- err
				return
			}
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

		// Close the sinkWriter
		if err := df.sinkWriter.Close(); err != nil {
			log.Errorw("Failed to close sink writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", df.sinkWriter.GetName()))
		} else {
			log.Infow("Closed sink writer", zap.String("sink", df.sinkWriter.GetName()))
		}

		// Close the fallback sinkWriter if configured
		if df.opts.fbSinkWriter != nil {
			if err := df.opts.fbSinkWriter.Close(); err != nil {
				log.Errorw("Failed to close fallback sink writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", df.opts.fbSinkWriter.GetName()))
			} else {
				log.Infow("Closed fallback sink writer", zap.String("sink", df.opts.fbSinkWriter.GetName()))
			}
		}

		close(stopped)
	}()

	return stopped
}

// forwardAChunk forwards a chunk of message from the fromBufferPartition to the sinkWriter. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to WhereTo, etc.
func (df *DataForward) forwardAChunk(ctx context.Context) error {
	// Initialize forwardAChunk and read start times
	start := time.Now()
	readStart := time.Now()
	totalBytes := 0
	dataBytes := 0
	// Initialize metric labels
	metricLabels := map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
	}
	metricLabelsWithPartition := map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
	}
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readMessages, err := df.fromBufferPartition.Read(ctx, df.opts.readBatchSize)
	df.opts.logger.Debugw("Read from buffer", zap.String("bufferFrom", df.fromBufferPartition.GetName()), zap.Int64("length", int64(len(readMessages))))
	if err != nil {
		df.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
		metrics.ReadMessagesError.With(metricLabelsWithPartition).Inc()
	}

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
			return nil
		}

		// if the validation passed, we will publish the idle watermark to SINK OT even though we do not use it today.
		idlehandler.PublishIdleWatermark(ctx, df.sinkWriter.GetPartitionIdx(), df.sinkWriter, df.wmPublisher, df.idleManager, df.opts.logger, df.vertexName, df.pipelineName, dfv1.VertexTypeSink, df.vertexReplica, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
		return nil
	}

	metrics.ReadProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(readStart).Microseconds()))

	var dataMessages = make([]*isb.ReadMessage, 0, len(readMessages))

	// store the offsets of the messages we read from ISB
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range readMessages {
		readOffsets[idx] = m.ReadOffset
		totalBytes += len(m.Payload)
		if m.Kind == isb.Data {
			dataMessages = append(dataMessages, m)
			dataBytes += len(m.Payload)
		}
	}

	metrics.ReadDataMessagesCount.With(metricLabelsWithPartition).Add(float64(len(dataMessages)))
	metrics.ReadMessagesCount.With(metricLabelsWithPartition).Add(float64(len(readMessages)))

	metrics.ReadBytesCount.With(metricLabelsWithPartition).Add(float64(totalBytes))

	metrics.ReadDataBytesCount.With(metricLabelsWithPartition).Add(float64(dataBytes))

	// fetch watermark if available
	// TODO: make it async (concurrent and wait later)
	// let's track only the first element's watermark. This is important because we reassign the watermark we fetch
	// to all the elements in the batch. If we were to assign last element's watermark, we will wrongly mark on-time data as late.
	// we fetch the watermark for the partition from which we read the message.
	processorWM := df.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, df.fromBufferPartition.GetPartitionIdx())

	writeMessages := make([]isb.Message, 0, len(dataMessages))
	for _, m := range dataMessages {
		m.Watermark = time.Time(processorWM)
		writeMessages = append(writeMessages, m.Message)
	}

	// write the messages to the sink
	_, fallbackMessages, err := df.writeToSink(ctx, df.sinkWriter, writeMessages, false)
	// error will not be nil only when we get ctx.Done()
	if err != nil {
		df.opts.logger.Errorw("failed to write to sink", zap.Error(err))
		df.fromBufferPartition.NoAck(ctx, readOffsets)
		return err
	}

	// Only when fallback is configured, it is possible to return fallbackMessages. If there's any, write to the fallback sink.
	if len(fallbackMessages) > 0 {
		df.opts.logger.Infow("Writing messages to fallback sink", zap.Int("count", len(fallbackMessages)))
		// write to sink is an infinite loop; it will return only if writes are successful or
		// ctx.Done happens due to shutdown.
		_, _, err = df.writeToSink(ctx, df.opts.fbSinkWriter, fallbackMessages, true)
		if err != nil {
			df.opts.logger.Errorw("Failed to write to fallback sink", zap.Error(err))
			df.fromBufferPartition.NoAck(ctx, readOffsets)
			return err
		}
	}

	// Always publish the watermark to SINK OT even though we do not use it today.
	// There's no offset returned from sink writer.
	df.wmPublisher.PublishWatermark(processorWM, nil, int32(0))
	// reset because the toBuffer is no longer idling
	df.idleManager.MarkActive(df.fromBufferPartition.GetPartitionIdx(), df.sinkWriter.GetName())

	df.opts.logger.Debugw("Write to sink completed")

	ackStart := time.Now()
	err = df.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		df.opts.logger.Errorw("Failed to ack from buffer", zap.Error(err))
		metrics.AckMessageError.With(metricLabelsWithPartition).Add(float64(len(readOffsets)))
		return nil
	}

	// Ack processing time
	metrics.AckProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(ackStart).Microseconds()))
	metrics.AckMessagesCount.With(metricLabelsWithPartition).Add(float64(len(readOffsets)))

	if df.opts.cbPublisher != nil {
		if err = df.opts.cbPublisher.SinkVertexCallback(ctx, writeMessages); err != nil {
			df.opts.logger.Error("Failed to execute callback", zap.Error(err))
		}
	}
	// ProcessingTimes of the entire forwardAChunk
	metrics.ForwardAChunkProcessingTime.With(metricLabels).Observe(float64(time.Since(start).Microseconds()))
	return nil
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

// writeToSink forwards an array of messages to a sink, and it is a blocking call it keeps retrying
// until shutdown has been initiated. The function also evaluates whether to use a fallback sink based
// on the error and configuration.
func (df *DataForward) writeToSink(ctx context.Context, sinkWriter sinker.SinkWriter, messagesToTry []isb.Message, isFbSinkWriter bool) ([]isb.Offset, []isb.Message, error) {
	var (
		err              error
		writeCount       int
		writeBytes       float64
		fallbackMessages []isb.Message
	)
	writeStart := time.Now()
	// slice to store the successful offsets returned by the sink
	writeOffsets := make([]isb.Offset, 0, len(messagesToTry))

	// extract the backOff conditions and failStrategy for the retry logic,
	// when the isFbSinkWriter is true, we use an infinite retry
	backoffCond, failStrategy := df.getBackOffConditions(isFbSinkWriter)

	// The loop will continue trying to write messages until they are all processed
	// or an unrecoverable error occurs.
	for {
		err = wait.ExponentialBackoffWithContext(ctx, backoffCond, func(_ context.Context) (done bool, err error) {
			// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation
			// since using failedMessages is an unlikely path.
			var failedMessages []isb.Message
			needRetry := false
			_writeOffsets, errs := sinkWriter.Write(ctx, messagesToTry)
			for idx, msg := range messagesToTry {
				if err = errs[idx]; err != nil {
					var udsinkErr = new(udsink.ApplyUDSinkErr)
					if errors.As(err, &udsinkErr) {
						if udsinkErr.IsInternalErr() {
							return false, err
						}
					}
					// if we are asked to write to fallback sink, check if the fallback sink is configured,
					// and we are not already in the fallback sink write path.
					if errors.Is(err, udsink.WriteToFallbackErr) && df.opts.fbSinkWriter != nil && !isFbSinkWriter {
						fallbackMessages = append(fallbackMessages, msg)
						continue
					}

					// if we are asked to write to fallback but no fallback sink is configured, we will retry the messages to the same sink
					if errors.Is(err, udsink.WriteToFallbackErr) && df.opts.fbSinkWriter == nil {
						df.opts.logger.Error("Asked to write to fallback but no fallback sink is configured, retrying the message to the same sink")
					}

					// if we are asked to write to fallback sink inside the fallback sink, we will retry the messages to the fallback sink
					if errors.Is(err, udsink.WriteToFallbackErr) && isFbSinkWriter {
						df.opts.logger.Error("Asked to write to fallback sink inside the fallback sink, retrying the message to fallback sink")
					}

					needRetry = true

					// TODO(Retry-Sink) : Propagate the retry-count?
					// we retry only failed message
					failedMessages = append(failedMessages, msg)

					// increment the error metric
					df.incrementErrorMetric(sinkWriter.GetName(), isFbSinkWriter)

					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := df.IsShuttingDown(); ok {
						metrics.PlatformError.With(map[string]string{metrics.LabelVertex: df.vertexName, metrics.LabelPipeline: df.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeSink), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica))}).Inc()
						return true, fmt.Errorf("writeToSink failed, Stop called while stuck on an internal error with failed messages:%d, %v", len(failedMessages), errs)
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
			// set messages to failedMessages, in case of success this should be empty
			// While checking for retry we see the length of the messages left
			messagesToTry = failedMessages
			if needRetry {
				df.opts.logger.Errorw("Retrying failed messages",
					zap.Any("errors", errorArrayToMap(errs)),
					zap.String(metrics.LabelPipeline, df.pipelineName),
					zap.String(metrics.LabelVertex, df.vertexName),
					zap.String(metrics.LabelPartitionName, sinkWriter.GetName()),
				)
				return false, nil
			}
			return true, nil
		})

		if ok, _ := df.IsShuttingDown(); err != nil && ok {
			return nil, nil, err
		}

		// Check what actions are required once the writing loop is completed
		// Break if no further action is required
		if !df.handlePostRetryFailures(&messagesToTry, failStrategy, &fallbackMessages, sinkWriter) {
			break
		}
	}
	// update the write metrics for sink
	df.updateSinkWriteMetrics(writeCount, writeBytes, sinkWriter.GetName(), isFbSinkWriter, writeStart)

	return writeOffsets, fallbackMessages, nil
}

// handlePostRetryFailures deals with the scenarios after retries are exhausted.
// It returns true if we need to continue retrying else returns false when no further writes are required
func (df *DataForward) handlePostRetryFailures(messagesToTry *[]isb.Message, failStrategy dfv1.OnFailureRetryStrategy, fallbackMessages *[]isb.Message,
	sinkWriter sinker.SinkWriter) bool {

	// Check if we still have messages left to be processed
	if len(*messagesToTry) > 0 {
		df.opts.logger.Infof("Retries exhausted in sink, messagesLeft %d, Next strategy %s",
			len(*messagesToTry), failStrategy)
		// Check what is the failure strategy to be followed after retry exhaustion
		switch failStrategy {
		case dfv1.OnFailureRetry:
			// If on failure, we keep on retrying then lets continue the loop and try all again
			return true
		case dfv1.OnFailureFallback:
			// If onFail we have to divert messages to fallback, lets add all failed messages to fallback slice
			*fallbackMessages = append(*fallbackMessages, *messagesToTry...)
		case dfv1.OnFailureDrop:
			// If on fail we want to Drop in that case lets not retry further
			df.opts.logger.Info("Dropping the failed messages after retry in the Sink")
			// Update the drop metric count with the messages left
			metrics.DropMessagesCount.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				metrics.LabelPartitionName:      sinkWriter.GetName(),
				metrics.LabelReason:             "retries exhausted in the Sink",
			}).Add(float64(len(*messagesToTry)))
		}
	}
	return false
}

// updateSinkWriteMetrics updates metrics related to data writes to a sink.
// Metrics are updated based on whether the operation involves the primary or fallback sink.
func (df *DataForward) updateSinkWriteMetrics(writeCount int, writeBytes float64, sinkWriterName string, isFallback bool, writeStart time.Time) {
	// Define labels to keep track of the data related to the specific operation
	labels := map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      sinkWriterName,
	}

	// Increment the metrics for message count and bytes
	metrics.WriteMessagesCount.With(labels).Add(float64(writeCount))
	metrics.WriteBytesCount.With(labels).Add(writeBytes)

	// Add write processing time metric
	metrics.WriteProcessingTime.With(labels).Observe(float64(time.Since(writeStart).Microseconds()))

	// if this is for Fallback Sink, increment specific metrics as well
	if isFallback {
		metrics.FbSinkWriteMessagesCount.With(labels).Add(float64(writeCount))
		metrics.FbSinkWriteBytesCount.With(labels).Add(writeBytes)
		metrics.FbSinkWriteProcessingTime.With(labels).Observe(float64(time.Since(writeStart).Microseconds()))
	}
}

// incrementErrorMetric updates the appropriate error metric based on whether the operation involves a fallback sink.
func (df *DataForward) incrementErrorMetric(sinkWriter string, isFbSinkWriter bool) {
	// Define labels to keep track of the data related to the specific operation
	labels := map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      sinkWriter,
	}

	// Increment the selected metric and attach labels to provide detailed context:
	metrics.WriteMessagesError.With(labels).Inc()

	// Increment fallback specific metric if fallback mode
	if isFbSinkWriter {
		metrics.FbSinkWriteMessagesError.With(labels).Inc()
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

// getBackOffConditions configures the retry backoff strategy based on whether its a fallbackSink or primary sink.
func (df *DataForward) getBackOffConditions(isFallbackSink bool) (wait.Backoff, dfv1.OnFailureRetryStrategy) {
	// If we want for isFallbackSink we will return an infinite retry which will keep retrying post exhaustion till it succeeds
	if isFallbackSink {
		return wait.Backoff{
			Duration: dfv1.DefaultRetryInterval,
			Steps:    dfv1.DefaultRetrySteps,
			Factor:   dfv1.DefaultFactor,
			Cap:      dfv1.DefaultMaxRetryInterval,
		}, dfv1.OnFailureRetry
	}
	// Initial interval duration and number of retries are taken from DataForward settings.
	return df.sinkRetryStrategy.GetBackoff(), df.sinkRetryStrategy.GetOnFailureRetryStrategy()
}

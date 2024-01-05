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

// Package reduce reads messages from isb and is responsible for the first part of reduce subsystem.
// It is responsible for feeding in the data to the second part of the reduce subsystem called processAndForward
// and processes the read data from the ISB, writes to the outbound channel called PBQ so the message can be
// asynchronously processed by `processAndForward`, and then closes the partition if possible based on watermark
// progression. To write to the outbound channel (PBQ), it has to partition the message first. Hence,
// also contains partitioning logic. Partitioner identifies a set of elements with a common key and time, and buckets
// them in to a common window. A partition is uniquely identified using a tuple {window, key}. Type of window does not
// matter. Partitioner is responsible for managing the persistence and processing of each partition. It uses PBQ for
// durable persistence of elements that belong to a partition and orchestrates the processing of elements using processAndForward
// function. Partitioner tracks active partitions, closes the partitions based on watermark progression and co-ordinates the
// materialization and forwarding the results to the next vertex in the pipeline.
package reduce

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
)

// DataForward is responsible for reading and forwarding the message from ISB to PBQ.
type DataForward struct {
	ctx                 context.Context
	vertexName          string
	pipelineName        string
	vertexReplica       int32
	fromBufferPartition isb.BufferReader
	toBuffers           map[string][]isb.BufferWriter
	wmFetcher           fetch.Fetcher
	wmPublishers        map[string]publish.Publisher
	windower            window.TimedWindower
	keyed               bool
	idleManager         wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid when the len(readMessage) is 0.
	wmbChecker     wmb.WMBChecker
	pbqManager     *pbq.Manager
	whereToDecider forwarder.ToWhichStepDecider
	of             *pnf.Manager
	opts           *Options
	log            *zap.SugaredLogger
}

// NewDataForward creates a new DataForward
func NewDataForward(ctx context.Context,
	vertexInstance *dfv1.VertexInstance,
	fromBuffer isb.BufferReader,
	toBuffers map[string][]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forwarder.ToWhichStepDecider,
	fw fetch.Fetcher,
	watermarkPublishers map[string]publish.Publisher,
	windowingStrategy window.TimedWindower,
	idleManager wmb.IdleManager,
	of *pnf.Manager,
	opts ...Option) (*DataForward, error) {

	options := DefaultOptions()

	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	rl := &DataForward{
		ctx:                 ctx,
		vertexName:          vertexInstance.Vertex.Spec.Name,
		pipelineName:        vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:       vertexInstance.Replica,
		fromBufferPartition: fromBuffer,
		toBuffers:           toBuffers,
		wmFetcher:           fw,
		wmPublishers:        watermarkPublishers,
		windower:            windowingStrategy,
		keyed:               vertexInstance.Vertex.Spec.UDF.GroupBy.Keyed,
		idleManager:         idleManager,
		pbqManager:          pbqManager,
		whereToDecider:      whereToDecider,
		of:                  of,
		wmbChecker:          wmb.NewWMBChecker(2), // TODO: make configurable
		log:                 logging.FromContext(ctx),
		opts:                options}

	return rl, nil
}

// Start starts reading messages from ISG
func (df *DataForward) Start() {

	for {
		select {
		case <-df.ctx.Done():

			// hard shutdown after timeout
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// allow to clean itself up.
			df.ShutDown(cctx)

			return
		default:
			// pass the child context so that the reader can be closed.
			// this way we can avoid the race condition and have all the read messages persisted
			// and acked.
			df.forwardAChunk(df.ctx)
		}
	}
}

// ReplayPersistedMessages replays persisted messages, because during boot up, it has to replay the data from the persistent store of
// PBQ before it can start reading from ISB. ReplayPersistedMessages will return only after the replay has been completed.
func (df *DataForward) ReplayPersistedMessages(ctx context.Context) error {
	// FIXME: fix replay for unaligned windows
	if df.windower.Type() == window.Unaligned {
		return nil
	}
	// start the PBQManager which discovers and builds the state from persistent store of the PBQ.
	partitions, err := df.pbqManager.GetExistingPartitions(ctx)
	if err != nil {
		return err
	}

	df.log.Infow("Partitions to be replayed ", zap.Int("count", len(partitions)), zap.Any("partitions", partitions))

	// for aligned windows, we can create/assign a partition based on the timestamp in the payload, but this is not
	// necessarily true for unaligned windows.

	for _, p := range partitions {
		// create a window for each partition and insert it to the windower
		// so that the window can be closed when the watermark
		// crosses the window.
		var timedWindow = window.NewWindowFromPartition(&p)

		df.windower.InsertWindow(timedWindow)

		df.associatePBQAndPnF(ctx, &p)
	}

	// replays the data (replay internally writes the data from persistent store on to the PBQ)
	df.pbqManager.Replay(ctx)

	return nil
}

// forwardAChunk reads a chunk of messages from isb and assigns watermark to messages
// and writes the windowRequests to pbq
func (df *DataForward) forwardAChunk(ctx context.Context) {
	readMessages, err := df.fromBufferPartition.Read(ctx, df.opts.readBatchSize)
	totalBytes := 0
	if err != nil {
		df.log.Errorw("Failed to read from isb", zap.Error(err))
		metrics.ReadMessagesError.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelPartitionName:      df.fromBufferPartition.GetName()}).Inc()
	}

	// idle watermark
	if len(readMessages) == 0 {
		// we get the Head idle wmb for the partition which we read the messages from and
		// use it as the idle watermark
		var processorWMB = df.wmFetcher.ComputeHeadIdleWMB(df.fromBufferPartition.GetPartitionIdx())
		if !df.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			df.log.Debugw("skip publishing idle watermark",
				zap.Int("counter", df.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("idle", processorWMB.Idle))
			return
		}

		// -1 means there are no windows
		// if there are no windows, and the len(readBatch) == 0
		// then it means there's an idle situation
		// in this case, send idle watermark to all the toBuffer partitions
		if oldestWindowEndTime := df.windower.OldestWindowEndTime(); oldestWindowEndTime == time.UnixMilli(-1) {
			for toVertexName, toVertexBuffer := range df.toBuffers {
				if publisher, ok := df.wmPublishers[toVertexName]; ok {
					for _, bufferPartition := range toVertexBuffer {
						idlehandler.PublishIdleWatermark(ctx, bufferPartition, publisher, df.idleManager, df.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
					}
				}
			}
		} else {
			// if window exists, and the watermark we fetch has already passed the endTime of this window
			// then it means the overall dataflow of the pipeline has already reached a later time point
			// so we can close the window and process the data in this window
			if watermark := time.UnixMilli(processorWMB.Watermark).Add(-1 * time.Millisecond); oldestWindowEndTime.Before(watermark) {
				windowOperations := df.windower.CloseWindows(watermark)
				for _, op := range windowOperations {
					err = df.writeToPBQ(ctx, op)
					if err != nil {
						df.log.Errorw("Failed to write to PBQ", zap.Error(err))
					}
				}
			} else {
				// if window exists, but the watermark we fetch is still within the endTime of the window
				// then we can't close the window because there could still be data after the idling situation ends
				// so in this case, we publish an idle watermark
				for toVertexName, toVertexBuffer := range df.toBuffers {
					if publisher, ok := df.wmPublishers[toVertexName]; ok {
						for _, bufferPartition := range toVertexBuffer {
							idlehandler.PublishIdleWatermark(ctx, bufferPartition, publisher, df.idleManager, df.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(watermark))
						}
					}
				}
			}
		}
		return
	}

	// fetch watermark using the first element's watermark, because we assign the watermark to all other
	// elements in the batch based on the watermark we fetch from 0th offset.
	// get the watermark for the partition from which we read the messages
	processorWM := df.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, df.fromBufferPartition.GetPartitionIdx())

	for _, m := range readMessages {
		if !df.keyed {
			m.Keys = []string{dfv1.DefaultKeyForNonKeyedData}
			m.Message.Keys = []string{dfv1.DefaultKeyForNonKeyedData}
		}
		m.Watermark = time.Time(processorWM)
		totalBytes += len(m.Payload)
	}
	metrics.ReadBytesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
	}).Add(float64(totalBytes))

	// readMessages has to be written to PBQ, acked, etc.
	df.process(ctx, readMessages)
}

// associatePBQAndPnF associates a PBQ with the partition if a PBQ exists, else creates a new one and then associates
// it to the partition.
func (df *DataForward) associatePBQAndPnF(ctx context.Context, partitionID *partition.ID) pbq.ReadWriteCloser {
	// look for existing pbq
	q := df.pbqManager.GetPBQ(*partitionID)

	// if we do not have already created PBQ, we have to create a new one.
	if q == nil {
		var pbqErr error
		var infiniteBackoff = wait.Backoff{
			Steps:    math.MaxInt,
			Duration: 1 * time.Second,
			Factor:   1.5,
			Jitter:   0.1,
		}
		pbqErr = wait.ExponentialBackoffWithContext(ctx, infiniteBackoff, func() (done bool, err error) {
			var attempt int
			q, pbqErr = df.pbqManager.CreateNewPBQ(ctx, *partitionID)
			if pbqErr != nil {
				attempt += 1
				df.log.Warnw("Failed to create pbq during startup, retrying", zap.Any("attempt", attempt), zap.String("partitionID", partitionID.String()), zap.Error(pbqErr))
				return false, nil
			}
			return true, nil
		})
		// since we created a brand new PBQ it means there is no PnF listening on this PBQ.
		// we should create and attach the read side of the loop (PnF) to the partition and then
		// start process-and-forward (pnf) loop
		df.of.AsyncSchedulePnF(ctx, partitionID, q)
		df.log.Debugw("Successfully Created/Found pbq and started PnF", zap.String("partitionID", partitionID.String()))
	}

	return q
}

// process is one iteration of the read loop which create/merges/closes windows and writes window requests to the PBQs followed by acking the messages, and
// then closing the windows that can closed.
func (df *DataForward) process(ctx context.Context, messages []*isb.ReadMessage) {
	var dataMessages = make([]*isb.ReadMessage, 0, len(messages))
	var ctrlMessages = make([]*isb.ReadMessage, 0) // for a high TPS pipeline, 0 is the most optimal value

	for _, message := range messages {
		if message.Kind == isb.Data {
			dataMessages = append(dataMessages, message)
		} else {
			ctrlMessages = append(ctrlMessages, message)
		}
	}
	metrics.ReadDataMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
	}).Add(float64(len(dataMessages)))
	metrics.ReadMessagesCount.With(map[string]string{
		metrics.LabelVertex:             df.vertexName,
		metrics.LabelPipeline:           df.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
	}).Add(float64(len(messages)))

	// write messages to windows based by PBQs.
	successfullyWrittenMessages, failedMessages, err := df.writeMessagesToWindows(ctx, dataMessages)
	if err != nil {
		df.log.Errorw("Failed to write messages", zap.Int("totalMessages", len(messages)), zap.Int("writtenMessage", len(successfullyWrittenMessages)))
	}

	// ack the control messages
	if len(ctrlMessages) != 0 {
		df.ackMessages(ctx, ctrlMessages)
	}

	if len(successfullyWrittenMessages) == 0 {
		return
	}

	// ack successful messages
	df.ackMessages(ctx, successfullyWrittenMessages)

	// no-ack the failed messages, so that they will be retried in the next iteration
	// if we don't do this, the failed messages will be retried after the ackWait time
	// which will cause correctness issues. We want these messages to be immediately retried.
	// When a message is retried, the offset remains the same, so an old message might jump out of the offset-timeline and cause the watermark to be -1.
	// The correctness is violated because the queue offset and message order are no longer monotonically increasing for those failed messages.
	df.noAckMessages(ctx, failedMessages)

	// close any windows that need to be closed.
	// since the watermark will be same for all the messages in the batch
	// we can invoke remove windows only once per batch
	wm := wmb.Watermark(successfullyWrittenMessages[0].Watermark)

	closedWindowOps := df.windower.CloseWindows(time.Time(wm).Add(-1 * df.opts.allowedLateness))

	df.log.Debugw("Windows eligible for closing", zap.Int("length", len(closedWindowOps)), zap.Time("watermark", time.Time(wm)))

	// write the close window operations to PBQ
	for _, winOp := range closedWindowOps {
		err = df.writeToPBQ(ctx, winOp)
		if err != nil {
			df.log.Errorw("Failed to write close signal to PBQ", zap.Error(err))
		}
	}

	// solve Reduce withholding of watermark where we do not send WM until the window is closed.
	oldestWindowEndTime := df.windower.OldestWindowEndTime()
	if oldestWindowEndTime != time.UnixMilli(-1) {
		// minus 1 ms because if it's the same as the end time the window would have already been closed
		if watermark := time.Time(wm).Add(-1 * time.Millisecond); oldestWindowEndTime.After(watermark) {
			// publish idle watermark so that the next vertex doesn't need to wait for the window to close to
			// start processing data whose watermark is smaller than the endTime of the toBeClosed window

			// this is to minimize watermark latency
			for toVertexName, toVertexBuffer := range df.toBuffers {
				if publisher, ok := df.wmPublishers[toVertexName]; ok {
					for _, bufferPartition := range toVertexBuffer {
						idlehandler.PublishIdleWatermark(ctx, bufferPartition, publisher, df.idleManager, df.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(watermark))
					}
				}
			}
		}
	}
}

// writeMessagesToWindows write the messages to each window that message belongs to. Each window is backed by a PBQ.
func (df *DataForward) writeMessagesToWindows(ctx context.Context, messages []*isb.ReadMessage) ([]*isb.ReadMessage, []*isb.ReadMessage, error) {
	var err error
	var writtenMessages = make([]*isb.ReadMessage, 0, len(messages))
	var failedMessages = make([]*isb.ReadMessage, 0)

messagesLoop:
	for i, message := range messages {
		if df.shouldDropMessage(message) {
			writtenMessages = append(writtenMessages, message)
			continue
		}

		// identify and add window for the message
		windowOperations := df.windower.AssignWindows(message)

		// for each window we will have a PBQ. A message could belong to multiple windows (e.g., sliding).
		// We need to write the messages to these PBQs
		for _, winOp := range windowOperations {

			err := df.writeToPBQ(ctx, winOp)
			// there is no point continuing because we are seeing an error.
			// this error will ONLY BE set if we are in an error loop and ctx.Done() has been invoked.
			if err != nil {
				df.log.Errorw("Failed to write message, asked to stop trying", zap.Any("msgOffSet", message.ReadOffset.String()), zap.String("partitionID", winOp.ID.String()), zap.Error(err))
				failedMessages = append(failedMessages, messages[i:]...)
				break messagesLoop
			}
		}

		writtenMessages = append(writtenMessages, message)
	}
	return writtenMessages, failedMessages, err
}

func (df *DataForward) shouldDropMessage(message *isb.ReadMessage) bool {
	if message.IsLate {
		// we should be able to get the late message in as long as there is an open window
		nextWinAsSeenByWriter := df.windower.NextWindowToBeClosed()
		// if there is no window open, drop the message
		if nextWinAsSeenByWriter == nil {
			df.log.Warnw("Dropping the late message", zap.Time("eventTime", message.EventTime), zap.Time("watermark", message.Watermark))
			return true
		} else if message.EventTime.Before(nextWinAsSeenByWriter.StartTime()) { // if the message doesn't fall in the next window that is about to be closed drop it.
			df.log.Warnw("Dropping the late message", zap.Time("eventTime", message.EventTime), zap.Time("watermark", message.Watermark), zap.Time("nextWindowToBeClosed", nextWinAsSeenByWriter.StartTime()))
			metrics.ReduceDroppedMessagesCount.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
				metrics.LabelReason:             "late"}).Inc()
			return true

			// mark it as a successfully written message as the message will be acked to avoid subsequent retries
		} else { // if the message falls in the next window that is about to be closed, keep it
			df.log.Debugw("Keeping the late message for next condition check because COB has not happened yet", zap.Int64("eventTime", message.EventTime.UnixMilli()), zap.Int64("watermark", message.Watermark.UnixMilli()), zap.Int64("nextWindowToBeClosed.startTime", nextWinAsSeenByWriter.StartTime().UnixMilli()))
		}
	}

	// We will accept data as long as window is open. If a straggler (late data) makes in before the window is closed,
	// it is accepted.

	// NOTE(potential bug): if we get a message where the event-time is < (watermark-allowedLateness), skip processing the message.
	// This could be due to a couple of problem, eg. ack was not registered, etc.
	// Please do not confuse this with late data! This is a platform related problem causing the watermark inequality
	// to be violated.
	if !message.IsLate && message.EventTime.Before(message.Watermark.Add(-1*df.opts.allowedLateness)) {
		// TODO: track as a counter metric
		df.log.Errorw("An old message just popped up", zap.Any("msgOffSet", message.ReadOffset.String()), zap.Int64("eventTime", message.EventTime.UnixMilli()), zap.Int64("watermark", message.Watermark.UnixMilli()), zap.Any("message", message.Message))
		// mark it as a successfully written message as the message will be acked to avoid subsequent retries
		// let's not continue processing this message, most likely the window has already been closed and the message
		// won't be processed anyways.
		metrics.ReduceDroppedMessagesCount.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			metrics.LabelReason:             "watermark_issue"}).Inc()
		return true
	}

	return false
}

// writeToPBQ writes to the PBQ. It will return error only if it is not failing to write to PBQ and is in a continuous
// error loop, and we have received ctx.Done() via SIGTERM.
func (df *DataForward) writeToPBQ(ctx context.Context, winOp *window.TimedWindowRequest) error {
	defer func(t time.Time) {
		metrics.PBQWriteTime.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		}).Observe(float64(time.Since(t).Milliseconds()))
	}(time.Now())

	var pbqWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	q := df.associatePBQAndPnF(ctx, winOp.ID)

	err := wait.ExponentialBackoff(pbqWriteBackoff, func() (done bool, err error) {
		rErr := q.Write(context.Background(), winOp)
		if rErr != nil {
			df.log.Errorw("Failed to write message", zap.String("msgOffSet", winOp.ReadMessage.ReadOffset.String()), zap.String("partitionID", winOp.ID.String()), zap.Error(rErr))
			metrics.PBQWriteErrorCount.With(map[string]string{
				metrics.LabelVertex:             df.vertexName,
				metrics.LabelPipeline:           df.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
			}).Inc()
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
		// happy path
		metrics.PBQWriteMessagesCount.With(map[string]string{
			metrics.LabelVertex:             df.vertexName,
			metrics.LabelPipeline:           df.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
		}).Inc()
		return true, nil
	})

	return err
}

// ackMessages acks messages. Retries until it can succeed or ctx.Done() happens.
func (df *DataForward) ackMessages(ctx context.Context, messages []*isb.ReadMessage) {
	var ackBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}
	var wg sync.WaitGroup

	// Ack the message to ISB
	for _, m := range messages {
		wg.Add(1)

		go func(o isb.Offset) {
			defer wg.Done()
			attempt := 0
			ackErr := wait.ExponentialBackoff(ackBackoff, func() (done bool, err error) {
				rErr := o.AckIt()
				attempt += 1
				if rErr != nil {
					metrics.AckMessageError.With(map[string]string{
						metrics.LabelVertex:             df.vertexName,
						metrics.LabelPipeline:           df.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
						metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
					}).Inc()

					df.log.Errorw("Failed to ack message, retrying", zap.String("msgOffSet", o.String()), zap.Error(rErr), zap.Int("attempt", attempt))
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
				df.log.Debugw("Successfully acked message", zap.String("msgOffSet", o.String()))
				metrics.AckMessagesCount.With(map[string]string{
					metrics.LabelVertex:             df.vertexName,
					metrics.LabelPipeline:           df.pipelineName,
					metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(df.vertexReplica)),
					metrics.LabelPartitionName:      df.fromBufferPartition.GetName(),
				}).Inc()
				return true, nil
			})
			// no point trying the rest of the message, most likely that will also fail
			if ackErr != nil {
				df.log.Errorw("Context closed while waiting to ack messages inside data forward", zap.Error(ackErr), zap.String("offset", o.String()))

			}
		}(m.ReadOffset)

	}
	wg.Wait()
}

// noAckMessages no-acks all the read offsets of failed messages.
func (df *DataForward) noAckMessages(ctx context.Context, failedMessages []*isb.ReadMessage) {
	var readOffsets []isb.Offset
	for _, m := range failedMessages {
		readOffsets = append(readOffsets, m.ReadOffset)
	}
	df.fromBufferPartition.NoAck(ctx, readOffsets)
}

// ShutDown shutdowns the read-loop.
func (df *DataForward) ShutDown(ctx context.Context) {

	df.log.Infow("Stopping reduce data forwarder...")

	if err := df.fromBufferPartition.Close(); err != nil {
		df.log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
	} else {
		df.log.Infow("Closed buffer reader", zap.String("bufferFrom", df.fromBufferPartition.GetName()))
	}

	// flush pending messages to persistent storage
	df.pbqManager.ShutDown(ctx)
}

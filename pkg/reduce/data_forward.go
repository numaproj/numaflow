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

	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
)

// DataForward is responsible for reading and forwarding the message from ISB to PBQ.
type DataForward struct {
	ctx                   context.Context
	vertexName            string
	pipelineName          string
	vertexReplica         int32
	fromBuffer            isb.BufferReader
	toBuffers             map[string]isb.BufferWriter
	watermarkFetcher      fetch.Fetcher
	watermarkPublishers   map[string]publish.Publisher
	windower              window.Windower
	keyed                 bool
	idleManager           *wmb.IdleManager
	wmbChecker            wmb.WMBChecker
	pbqManager            *pbq.Manager
	whereToDecider        forward.ToWhichStepDecider
	udfInvocationTracking map[partition.ID]*pnf.ForwardTask
	of                    *pnf.OrderedProcessor
	opts                  *Options
	log                   *zap.SugaredLogger
}

func NewDataForward(ctx context.Context,
	vertexInstance *dfv1.VertexInstance,
	fromBuffer isb.BufferReader,
	toBuffers map[string]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forward.ToWhichStepDecider,
	fw fetch.Fetcher,
	watermarkPublishers map[string]publish.Publisher,
	windowingStrategy window.Windower,
	idleManager *wmb.IdleManager,
	of *pnf.OrderedProcessor,
	opts ...Option) (*DataForward, error) {

	options := DefaultOptions()

	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	rl := &DataForward{
		ctx:                   ctx,
		vertexName:            vertexInstance.Vertex.Spec.Name,
		pipelineName:          vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:         vertexInstance.Replica,
		fromBuffer:            fromBuffer,
		toBuffers:             toBuffers,
		watermarkFetcher:      fw,
		watermarkPublishers:   watermarkPublishers,
		windower:              windowingStrategy,
		keyed:                 vertexInstance.Vertex.Spec.UDF.GroupBy.Keyed,
		idleManager:           idleManager,
		pbqManager:            pbqManager,
		whereToDecider:        whereToDecider,
		udfInvocationTracking: make(map[partition.ID]*pnf.ForwardTask),
		of:                    of,
		wmbChecker:            wmb.NewWMBChecker(2), // TODO: make configurable
		log:                   logging.FromContext(ctx),
		opts:                  options}

	return rl, nil
}

// Start starts reading messages from ISG
func (rl *DataForward) Start() {
	for {
		select {
		case <-rl.ctx.Done():
			rl.log.Infow("Stopping reduce data forwarder...")

			if err := rl.fromBuffer.Close(); err != nil {
				rl.log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
			} else {
				rl.log.Infow("Closed buffer reader", zap.String("bufferFrom", rl.fromBuffer.GetName()))
			}

			// hard shutdown after timeout
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// allow to clean itself up.
			rl.ShutDown(cctx)

			for _, v := range rl.toBuffers {
				if err := v.Close(); err != nil {
					rl.log.Errorw("Failed to close buffer writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", v.GetName()))
				} else {
					rl.log.Infow("Closed buffer writer", zap.String("bufferTo", v.GetName()))
				}
			}

			// stop watermark fetcher
			if err := rl.watermarkFetcher.Close(); err != nil {
				rl.log.Errorw("Failed to close watermark fetcher", zap.Error(err))
			}

			// stop watermark publisher
			for _, publisher := range rl.watermarkPublishers {
				if err := publisher.Close(); err != nil {
					rl.log.Errorw("Failed to close watermark publisher", zap.Error(err))
				}
			}
			return
		default:
			// pass the child context so that the reader can be closed.
			// this way we can avoid the race condition and have all the read messages persisted
			// and acked.
			rl.forwardAChunk(rl.ctx)
		}
	}
}

// Startup starts up the read-loop, because during boot up, it has to replay the data from the persistent store of
// PBQ before it can start reading from ISB. Startup will return only after the replay has been completed.
func (rl *DataForward) Startup(ctx context.Context) error {
	// start the PBQManager which discovers and builds the state from persistent store of the PBQ.
	partitions, err := rl.pbqManager.GetExistingPartitions(ctx)
	if err != nil {
		return err
	}

	rl.log.Infow("Partitions to be replayed ", zap.Int("count", len(partitions)), zap.Any("partitions", partitions))

	for _, p := range partitions {
		// Create keyed window for a given partition
		// so that the window can be closed when the watermark
		// crosses the window.

		alignedKeyedWindow := keyed.NewKeyedWindow(p.Start, p.End)

		// insert the window to the list of active windows, since the active window list is in-memory
		keyedWindow, _ := rl.windower.InsertIfNotPresent(alignedKeyedWindow)

		// add slots to the window, so that when a new message with the watermark greater than
		// the window end time comes, slots will not be lost and the windows will be closed as expected
		keyedWindow.AddSlot(p.Slot)

		// create and invoke process and forward for the partition
		rl.associatePBQAndPnF(ctx, p, keyedWindow)
	}

	// replays the data (replay internally writes the data from persistent store on to the PBQ)
	rl.pbqManager.Replay(ctx)
	return nil
}

// forwardAChunk reads a chunk of messages from isb and assigns watermark to messages
// and writes the messages to pbq
func (rl *DataForward) forwardAChunk(ctx context.Context) {
	readMessages, err := rl.fromBuffer.Read(ctx, rl.opts.readBatchSize)
	totalBytes := 0
	if err != nil {
		rl.log.Errorw("Failed to read from isb", zap.Error(err))
		readMessagesError.With(map[string]string{
			metrics.LabelVertex:             rl.vertexName,
			metrics.LabelPipeline:           rl.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
			"buffer":                        rl.fromBuffer.GetName()}).Inc()
	}

	if len(readMessages) == 0 {
		// we use the HeadWMB as the watermark for the idle
		var processorWMB = rl.watermarkFetcher.GetHeadWMB()
		if !rl.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			rl.log.Debugw("skip publishing idle watermark",
				zap.Int("counter", rl.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("Idle", processorWMB.Idle))
			return
		}

		nextWin := rl.pbqManager.NextWindowToBeClosed()
		if nextWin == nil {
			// if all the windows are closed already, and the len(readBatch) == 0
			// then it means there's an idle situation
			// in this case, send idle watermark to all the toBuffers
			for _, toBuffer := range rl.toBuffers {
				if publisher, ok := rl.watermarkPublishers[toBuffer.GetName()]; ok {
					idlehandler.PublishIdleWatermark(ctx, toBuffer, publisher, rl.idleManager, rl.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
				}
			}
		} else {
			// if toBeClosed window exists, and the watermark we fetch has already passed the endTime of this window
			// then it means the overall dataflow of the pipeline has already reached a later time point
			// so we can close the window and process the data in this window
			if watermark := time.UnixMilli(processorWMB.Watermark).Add(-1 * time.Millisecond); nextWin.EndTime().Before(watermark) {
				closedWindows := rl.windower.RemoveWindows(watermark)
				for _, win := range closedWindows {
					rl.ClosePartitions(win.Partitions())
				}
			} else {
				// if toBeClosed window exists, but the watermark we fetch is still within the endTime of the window
				// then we can't close the window because there could still be data after the idling situation ends
				// so in this case, we publish an idle watermark
				for _, toBuffer := range rl.toBuffers {
					if publisher, ok := rl.watermarkPublishers[toBuffer.GetName()]; ok {
						idlehandler.PublishIdleWatermark(ctx, toBuffer, publisher, rl.idleManager, rl.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(watermark))
					}
				}
			}
		}
		return
	}
	readMessagesCount.With(map[string]string{
		metrics.LabelVertex:             rl.vertexName,
		metrics.LabelPipeline:           rl.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
		"buffer":                        rl.fromBuffer.GetName(),
	}).Add(float64(len(readMessages)))
	// fetch watermark using the first element's watermark, because we assign the watermark to all other
	// elements in the batch based on the watermark we fetch from 0th offset.
	processorWM := rl.watermarkFetcher.GetWatermark(readMessages[0].ReadOffset)
	for _, m := range readMessages {
		if !rl.keyed {
			m.Keys = []string{dfv1.DefaultKeyForNonKeyedData}
			m.Message.Keys = []string{dfv1.DefaultKeyForNonKeyedData}
		}
		m.Watermark = time.Time(processorWM)
		totalBytes += len(m.Payload)
	}
	readBytesCount.With(map[string]string{
		metrics.LabelVertex:             rl.vertexName,
		metrics.LabelPipeline:           rl.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
		"buffer":                        rl.fromBuffer.GetName(),
	}).Add(float64(totalBytes))

	// readMessages has to be written to PBQ, acked, etc.
	rl.Process(ctx, readMessages)
}

// associatePBQAndPnF associates a PBQ with the partition if a PBQ exists, else creates a new one and then associates
// it to the partition.
func (rl *DataForward) associatePBQAndPnF(ctx context.Context, partitionID partition.ID, kw window.AlignedKeyedWindower) pbq.ReadWriteCloser {
	// look for existing pbq
	q := rl.pbqManager.GetPBQ(partitionID)

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
			q, pbqErr = rl.pbqManager.CreateNewPBQ(ctx, partitionID, kw)
			if pbqErr != nil {
				attempt += 1
				rl.log.Warnw("Failed to create pbq during startup, retrying", zap.Any("attempt", attempt), zap.String("partitionID", partitionID.String()), zap.Error(pbqErr))
				return false, nil
			}
			return true, nil
		})
		// since we created a brand new PBQ it means there is no PnF listening on this PBQ.
		// we should create and attach the read side of the loop (PnF) to the partition and then
		// start process-and-forward (pnf) loop
		t := rl.of.SchedulePnF(ctx, partitionID)
		rl.udfInvocationTracking[partitionID] = t
		rl.log.Debugw("Successfully Created/Found pbq and started PnF", zap.String("partitionID", partitionID.String()))
	}

	return q
}

// Process is one iteration of the read loop which writes the messages to the PBQs followed by acking the messages, and
// then closing the windows that can closed.
func (rl *DataForward) Process(ctx context.Context, messages []*isb.ReadMessage) {
	var dataMessages = make([]*isb.ReadMessage, 0, len(messages))
	var ctrlMessages = make([]*isb.ReadMessage, 0) // for a high TPS pipeline, 0 is the most optimal value

	for _, message := range messages {
		if message.Kind == isb.Data {
			dataMessages = append(dataMessages, message)
		} else {
			ctrlMessages = append(ctrlMessages, message)
		}
	}

	// write messages to windows based by PBQs.
	successfullyWrittenMessages, err := rl.writeMessagesToWindows(ctx, dataMessages)
	if err != nil {
		rl.log.Errorw("Failed to write messages", zap.Int("totalMessages", len(messages)), zap.Int("writtenMessage", len(successfullyWrittenMessages)))
	}

	// ack the control messages
	if len(ctrlMessages) != 0 {
		rl.ackMessages(ctx, ctrlMessages)
	}

	if len(successfullyWrittenMessages) == 0 {
		return
	}
	// ack successful messages
	rl.ackMessages(ctx, successfullyWrittenMessages)

	// close any windows that need to be closed.
	// since the watermark will be same for all the messages in the batch
	// we can invoke remove windows only once per batch
	var closedWindows []window.AlignedKeyedWindower
	wm := wmb.Watermark(successfullyWrittenMessages[0].Watermark)

	closedWindows = rl.windower.RemoveWindows(time.Time(wm).Add(-1 * rl.opts.allowedLateness))

	rl.log.Debugw("Windows eligible for closing", zap.Int("length", len(closedWindows)), zap.Time("watermark", time.Time(wm)))

	for _, cw := range closedWindows {
		partitions := cw.Partitions()
		rl.ClosePartitions(partitions)
		rl.log.Debugw("Closing Window", zap.Int64("windowStart", cw.StartTime().UnixMilli()), zap.Int64("windowEnd", cw.EndTime().UnixMilli()))
	}

	// solve Reduce withholding of watermark where we do not send WM until the window is closed.
	if nextWin := rl.pbqManager.NextWindowToBeClosed(); nextWin != nil {
		// minus 1 ms because if it's the same as the end time the window would have already been closed
		if watermark := time.Time(wm).Add(-1 * time.Millisecond); nextWin.EndTime().After(watermark) {
			// publish idle watermark so that the next vertex doesn't need to wait for the window to close to
			// start processing data whose watermark is smaller than the endTime of the toBeClosed window

			// this is to minimize watermark latency
			for _, toBuffer := range rl.toBuffers {
				if publisher, ok := rl.watermarkPublishers[toBuffer.GetName()]; ok {
					idlehandler.PublishIdleWatermark(ctx, toBuffer, publisher, rl.idleManager, rl.log, dfv1.VertexTypeReduceUDF, wmb.Watermark(watermark))
				}
			}
		}
	}
}

// writeMessagesToWindows write the messages to each window that message belongs to. Each window is backed by a PBQ.
func (rl *DataForward) writeMessagesToWindows(ctx context.Context, messages []*isb.ReadMessage) ([]*isb.ReadMessage, error) {
	var err error
	var writtenMessages = make([]*isb.ReadMessage, 0, len(messages))

messagesLoop:
	for _, message := range messages {
		// drop the late messages only if there is no window open
		if message.IsLate {
			// we should be able to get the late message in as long as there is an open window
			nextWin := rl.pbqManager.NextWindowToBeClosed()
			if nextWin != nil && message.EventTime.Before(nextWin.StartTime()) {
				rl.log.Warnw("Dropping the late message", zap.Time("eventTime", message.EventTime), zap.Time("watermark", message.Watermark), zap.Time("nextWindowToBeClosed", nextWin.StartTime()))
				droppedMessagesCount.With(map[string]string{
					metrics.LabelVertex:             rl.vertexName,
					metrics.LabelPipeline:           rl.pipelineName,
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
					LabelReason:                     "late"}).Inc()

				// mark it as a successfully written message as the message will be acked to avoid subsequent retries
				writtenMessages = append(writtenMessages, message)
				continue
			} else {
				var startTime time.Time
				// bit of an overkill, but this is an unlikely path
				if nextWin == nil {
					startTime = time.Time{}
				} else {
					startTime = nextWin.StartTime()
				}
				rl.log.Debugw("Keeping the late message for next condition check because COB has not happened yet", zap.Int64("eventTime", message.EventTime.UnixMilli()), zap.Int64("watermark", message.Watermark.UnixMilli()), zap.Int64("nextWindowToBeClosed.startTime", startTime.UnixMilli()))
			}
		}

		// NOTE(potential bug): if we get a message where the event-time is < (watermark-allowedLateness), skip processing the message.
		// This could be due to a couple of problem, eg. ack was not registered, etc.
		// Please do not confuse this with late data! This is a platform related problem causing the watermark inequality
		// to be violated.
		if message.EventTime.Before(message.Watermark.Add(-1 * rl.opts.allowedLateness)) {
			// TODO: track as a counter metric
			rl.log.Errorw("An old message just popped up", zap.Any("msgOffSet", message.ReadOffset.String()), zap.Int64("eventTime", message.EventTime.UnixMilli()), zap.Int64("watermark", message.Watermark.UnixMilli()), zap.Any("message", message.Message))
			// mark it as a successfully written message as the message will be acked to avoid subsequent retries
			writtenMessages = append(writtenMessages, message)
			// let's not continue processing this message, most likely the window has already been closed and the message
			// won't be processed anyways.
			droppedMessagesCount.With(map[string]string{
				metrics.LabelVertex:             rl.vertexName,
				metrics.LabelPipeline:           rl.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
				LabelReason:                     "watermark_issue"}).Inc()
			continue
		}

		// identify and add window for the message
		windows := rl.upsertWindowsAndKeys(message)

		// for each window we will have a PBQ. A message could belong to multiple windows (e.g., sliding).
		// We need to write the messages to these PBQs.
		for _, kw := range windows {

			for _, partitionID := range kw.Partitions() {

				err := rl.writeToPBQ(ctx, message, partitionID, kw)
				// there is no point continuing because we are seeing an error.
				// this error will ONLY BE set if we are in a erroring loop and ctx.Done() has been invoked.
				if err != nil {
					rl.log.Errorw("Failed to write message, asked to stop trying", zap.Any("msgOffSet", message.ReadOffset.String()), zap.String("partitionID", partitionID.String()), zap.Error(err))
					break messagesLoop
				}
			}
		}

		writtenMessages = append(writtenMessages, message)
	}

	return writtenMessages, err
}

// writeToPBQ writes to the PBQ. It will return error only if it is not failing to write to PBQ and is in a continuous
// error loop, and we have received ctx.Done() via SIGTERM.
func (rl *DataForward) writeToPBQ(ctx context.Context, m *isb.ReadMessage, p partition.ID, kw window.AlignedKeyedWindower) error {
	startTime := time.Now()
	defer pbqWriteTime.With(map[string]string{
		metrics.LabelVertex:             rl.vertexName,
		metrics.LabelPipeline:           rl.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
	}).Observe(float64(time.Since(startTime).Milliseconds()))

	var pbqWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	q := rl.associatePBQAndPnF(ctx, p, kw)

	err := wait.ExponentialBackoff(pbqWriteBackoff, func() (done bool, err error) {
		rErr := q.Write(context.Background(), m)
		if rErr != nil {
			rl.log.Errorw("Failed to write message", zap.Any("msgOffSet", m.ReadOffset.String()), zap.String("partitionID", p.String()), zap.Error(rErr))
			pbqWriteErrorCount.With(map[string]string{
				metrics.LabelVertex:             rl.vertexName,
				metrics.LabelPipeline:           rl.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
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
		pbqWriteMessagesCount.With(map[string]string{
			metrics.LabelVertex:             rl.vertexName,
			metrics.LabelPipeline:           rl.pipelineName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
		}).Inc()
		rl.log.Debugw("Successfully wrote the message", zap.String("msgOffSet", m.ReadOffset.String()), zap.String("partitionID", p.String()))
		return true, nil
	})

	return err
}

// ackMessages acks messages. Retries until it can succeed or ctx.Done() happens.
func (rl *DataForward) ackMessages(ctx context.Context, messages []*isb.ReadMessage) {
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
					ackMessageError.With(map[string]string{
						metrics.LabelVertex:             rl.vertexName,
						metrics.LabelPipeline:           rl.pipelineName,
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
					}).Inc()

					rl.log.Errorw("Failed to ack message, retrying", zap.String("msgOffSet", o.String()), zap.Error(rErr), zap.Int("attempt", attempt))
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
				rl.log.Debugw("Successfully acked message", zap.String("msgOffSet", o.String()))
				ackMessagesCount.With(map[string]string{
					metrics.LabelVertex:             rl.vertexName,
					metrics.LabelPipeline:           rl.pipelineName,
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(rl.vertexReplica)),
				}).Inc()
				return true, nil
			})
			// no point trying the rest of the message, most likely that will also fail
			if ackErr != nil {
				rl.log.Errorw("Context closed while waiting to ack messages inside data forward", zap.Error(ackErr), zap.String("offset", o.String()))

			}
		}(m.ReadOffset)

	}
	wg.Wait()
}

// ShutDown shutdowns the read-loop.
func (rl *DataForward) ShutDown(ctx context.Context) {
	rl.pbqManager.ShutDown(ctx)
}

// upsertWindowsAndKeys will create or assigns (if already present) a window to the message. It is an upsert operation
// because windows are created out of order, but they will be closed in-order.
func (rl *DataForward) upsertWindowsAndKeys(m *isb.ReadMessage) []window.AlignedKeyedWindower {

	processingWindows := rl.windower.AssignWindow(m.EventTime)
	var kWindows []window.AlignedKeyedWindower
	for _, win := range processingWindows {
		w, isPresent := rl.windower.InsertIfNotPresent(win)
		if !isPresent {
			rl.log.Debugw("Creating new keyed window", zap.String("msg.offset", m.ID), zap.Int64("startTime", w.StartTime().UnixMilli()), zap.Int64("endTime", w.EndTime().UnixMilli()))
		} else {
			rl.log.Debugw("Found an existing window", zap.String("msg.offset", m.ID), zap.Int64("startTime", w.StartTime().UnixMilli()), zap.Int64("endTime", w.EndTime().UnixMilli()))
		}
		// track the key to window relationship
		// FIXME: create a slot from m.key
		w.AddSlot("slot-0")
		kWindows = append(kWindows, w)
	}
	return kWindows
}

// ClosePartitions closes the partitions by invoking close-of-book (COB).
func (rl *DataForward) ClosePartitions(partitions []partition.ID) {
	for _, p := range partitions {
		q := rl.pbqManager.GetPBQ(p)
		rl.log.Infow("Close of book", zap.String("partitionID", p.String()))
		// schedule the task for ordered processing.
		rl.of.InsertTask(rl.udfInvocationTracking[p])
		q.CloseOfBook()
		delete(rl.udfInvocationTracking, p)
	}
}

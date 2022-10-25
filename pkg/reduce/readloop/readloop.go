// Package readloop is responsible for the first part of reduce subsystem. It is responsible for feeding in the data
// to the second part of the reduce subsystem called ProcessAndForward. `readloop` processes the read data from the ISB,
// writes to the outbound channel called PBQ so the message can be asynchronously processed by `ProcessAndForward`, and
// then closes the partition if possible based on watermark progression. To write to the outbound channel (PBQ),
// `readloop` has to partition the message first. Hence, `readloop` also contains partitioning logic.
// Partitioner identifies a set of elements with a common key and time, and buckets them in to a common window. A
// partition is uniquely identified using a tuple {window, key}. Type of window does not matter. Partitioner is
// responsible for managing the persistence and processing of each partition. It uses PBQ for durable persistence of
// elements that belong to a partition and orchestrates the processing of elements using ProcessAndForward function.
// Partitioner tracks active partitions, closes the partitions based on watermark progression and co-ordinates the
// materialization and forwarding the results to the next vertex in the pipeline.
package readloop

import (
	"context"
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfReducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
)

// ReadLoop is responsible for reading and forwarding the message from ISB to PBQ.
type ReadLoop struct {
	UDF               udfReducer.Reducer
	pbqManager        *pbq.Manager
	windowingStrategy window.Windower
	aw                *fixed.ActiveWindows
	op                *orderedForwarder
	log               *zap.SugaredLogger
	toBuffers         map[string]isb.BufferWriter
	whereToDecider    forward.ToWhichStepDecider
	publishWatermark  map[string]publish.Publisher
}

// NewReadLoop initializes  and returns ReadLoop.
func NewReadLoop(ctx context.Context,
	udf udfReducer.Reducer,
	pbqManager *pbq.Manager,
	windowingStrategy window.Windower,
	toBuffers map[string]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider,
	pw map[string]publish.Publisher,
	_ *window.Options) *ReadLoop {

	op := newOrderedForwarder(ctx)

	rl := &ReadLoop{
		UDF:               udf,
		windowingStrategy: windowingStrategy,
		// TODO: pass window type
		aw:               fixed.NewWindows(),
		pbqManager:       pbqManager,
		op:               op,
		log:              logging.FromContext(ctx),
		toBuffers:        toBuffers,
		whereToDecider:   whereToDecider,
		publishWatermark: pw,
	}
	op.startUp(ctx)
	return rl
}

// Startup starts up the read-loop, because during boot up, it has to replay the data from the persistent store of
// PBQ before it can start reading from ISB. Startup will return only after the replay has been completed.
func (rl *ReadLoop) Startup(ctx context.Context) {
	// start the PBQManager which discovers and builds the state from persistent store of the PBQ.
	rl.pbqManager.StartUp(ctx)
	// gets the partitions from the state
	partitions := rl.pbqManager.ListPartitions()

	for _, p := range partitions {
		// Create keyed window for a given partition
		// so that the window can be closed when the watermark
		// crosses the window.
		id := p.PartitionID
		intervalWindow := &window.IntervalWindow{
			Start: id.Start,
			End:   id.End,
		}
		// These windows have to be recreated as they are completely in-memory
		rl.aw.CreateKeyedWindow(intervalWindow)

		// create and invoke process and forward for the partition
		rl.associatePBQAndPnF(ctx, p.PartitionID)
	}

	// replays the data (replay internally writes the data from persistent store on to the PBQ)
	rl.pbqManager.Replay(ctx)
}

// Process is one iteration of the read loop.
func (rl *ReadLoop) Process(ctx context.Context, messages []*isb.ReadMessage) {

	// There is no Cap on backoff because setting a Cap will result in
	// backoff stopped once the duration exceeds the Cap
	var pbqWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	for _, m := range messages {
		// identify and add window for the message
		var ctxClosedErr error
		windows := rl.upsertWindowsAndKeys(m)
		// for each window we will have a PBQ. A message could belong to multiple windows (e.g., sliding).
		for _, kw := range windows {
			// identify partition for message
			partitionID := partition.ID{
				Start: kw.IntervalWindow.Start,
				End:   kw.IntervalWindow.End,
				Key:   m.Key,
			}

			q := rl.associatePBQAndPnF(ctx, partitionID)

			// write the message to PBQ
			writeFn := func(ctx context.Context, m *isb.ReadMessage) error {
				return q.Write(ctx, m)
			}
			ctxClosedErr = rl.executeWithBackOff(ctx, writeFn, "failed to Write Message", pbqWriteBackoff, m, partitionID)
			if ctxClosedErr != nil {
				rl.log.Errorw("Error while writing the message to PBQ", zap.Error(ctxClosedErr))
				return
			}

			// Ack the message to ISB
			ackFn := func(_ context.Context, m *isb.ReadMessage) error {
				return m.ReadOffset.AckIt()
			}
			ctxClosedErr = rl.executeWithBackOff(ctx, ackFn, "failed to Ack Message", pbqWriteBackoff, m, partitionID)
			if ctxClosedErr != nil {
				rl.log.Errorw("Error while acknowledging the message", zap.Error(ctxClosedErr))
				return
			}
		}

		// close any windows that need to be closed.
		// FIXME(p0): why are we re-reading the watermark? isb.ReadMessage contains watermark.
		wm := rl.waterMark(m)
		closedWindows := rl.aw.RemoveWindow(time.Time(wm))
		rl.log.Debugw("closing windows", zap.Int("length", len(closedWindows)), zap.Time("watermark", time.Time(wm)))

		for _, cw := range closedWindows {
			partitions := cw.Partitions()
			rl.closePartitions(partitions)
		}
	}
}

// executeWithBackOff executes a function infinitely until it succeeds using ExponentialBackoffWithContext.
func (rl *ReadLoop) executeWithBackOff(ctx context.Context, retryableFn func(ctx context.Context, message *isb.ReadMessage) error, errMsg string, pbqWriteBackoff wait.Backoff, m *isb.ReadMessage, partitionID partition.ID) error {
	attempt := 0
	ctxClosedErr := wait.ExponentialBackoffWithContext(ctx, pbqWriteBackoff, func() (done bool, err error) {
		rErr := retryableFn(ctx, m)
		attempt += 1
		if rErr != nil {
			rl.log.Errorw(errMsg, zap.Any("msgOffSet", m.ReadOffset.String()), zap.Any("partitionID", partitionID.String()), zap.Any("attempt", attempt), zap.Error(rErr))
			return false, nil
		}
		return true, nil
	})

	return ctxClosedErr
}

// associatePBQAndPnF associates a PBQ with the partition if a PBQ exists, else creates a new one and then associates
// it to the partition.
func (rl *ReadLoop) associatePBQAndPnF(ctx context.Context, partitionID partition.ID) pbq.ReadWriteCloser {
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
			q, pbqErr = rl.pbqManager.CreateNewPBQ(ctx, partitionID)
			if pbqErr != nil {
				attempt += 1
				rl.log.Warnw("Failed to create pbq during startup, retrying", zap.Any("attempt", attempt), zap.Any("partitionID", partitionID.String()), zap.Error(pbqErr))
				return false, nil
			}
			return true, nil
		})
		// since we created a brand new PBQ it means there is no PnF listening on this PBQ.
		// we should create and attach the read side of the loop (PnF) to the partition and then
		// start process-and-forward (pnf) loop
		rl.op.schedulePnF(ctx, rl.UDF, q, partitionID, rl.toBuffers, rl.whereToDecider, rl.publishWatermark)
	}
	return q
}

// ShutDown shutdowns the read-loop.
func (rl *ReadLoop) ShutDown(ctx context.Context) {
	rl.pbqManager.ShutDown(ctx)
}

// upsertWindowsAndKeys will create or assigns (if already present) a window to the message. It is an upsert operation
// because windows are created out of order, but they will be closed in-order.
func (rl *ReadLoop) upsertWindowsAndKeys(m *isb.ReadMessage) []*keyed.KeyedWindow {
	processingWindows := rl.windowingStrategy.AssignWindow(m.EventTime)
	var kWindows []*keyed.KeyedWindow
	for _, win := range processingWindows {
		kw := rl.aw.GetKeyedWindow(win)
		if kw == nil {
			kw = rl.aw.CreateKeyedWindow(win)
		}
		// track the key to window relationship
		kw.AddKey(m.Key)
		kWindows = append(kWindows, kw)
	}
	return kWindows
}

// FIXME: this shouldn't be required.
func (rl *ReadLoop) waterMark(message *isb.ReadMessage) processor.Watermark {
	return processor.Watermark(message.Watermark)
}

// closePartitions closes the partitions by invoking close-of-book (COB).
func (rl *ReadLoop) closePartitions(partitions []partition.ID) {
	for _, p := range partitions {
		q := rl.pbqManager.GetPBQ(p)
		q.CloseOfBook()
	}
}

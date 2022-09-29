// Package readloop contains partitioning logic. A partition identifies a set of elements with a common key and
// are bucketed in to a common window. A partition is uniquely identified using a tuple {window, key}. Type of window
// does not matter.
// partitioner is responsible for managing the persistence and processing of each partition.
// It uses PBQ for durable persistence of elements that belong to a partition and orchestrates the processing of
// elements using ProcessAndForward function.
// partitioner tracks active partitions, closes the partitions based on watermark progression and co-ordinates the
// materialization and forwarding the results to the next vertex in the pipeline.
package readloop

import (
	"context"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
)

var retryDelay time.Duration = time.Duration(1 * time.Second)

type ReadLoop struct {
	pbqManager        *pbq.Manager
	windowingStrategy window.Windower
	aw                *fixed.ActiveWindows
	op                *orderedProcessor
}

// NewReadLoop initializes ReadLoop struct
func NewReadLoop(ctx context.Context, pbqManager *pbq.Manager, windowingStrategy window.Windower) *ReadLoop {

	op := NewOrderedProcessor()

	rl := &ReadLoop{
		windowingStrategy: windowingStrategy,
		// TODO pass window type
		aw:         fixed.NewWindows(),
		pbqManager: pbqManager,
		op:         op,
	}
	op.StartUp(ctx)
	return rl
}

func (rl *ReadLoop) Startup(ctx context.Context) {
	// at this point, it is assumed that pbq manager has been initialized
	// and that it is ready for use so start it up.
	rl.pbqManager.StartUp(ctx)
	// replay the partitions
	partitions := rl.pbqManager.ListPartitions()
	for _, p := range partitions {
		// create process and forward
		// invoke process and forward with partition
		rl.processPartition(ctx, p.PartitionID)
	}
	rl.pbqManager.Replay(ctx)
}

func (rl *ReadLoop) Process(ctx context.Context, messages []*isb.ReadMessage) {
	for _, m := range messages {
		// identify and add window for the message
		windows := rl.upsertWindowsAndKeys(m)
		for _, kw := range windows {
			// identify partition for message
			partitionID := partition.ID{
				Start: kw.IntervalWindow.Start,
				End:   kw.IntervalWindow.End,
				Key:   m.Key,
			}
			//(kw.IntervalWindow, m.Key)

			q := rl.processPartition(ctx, partitionID)
			// write the message to PBQ
			q.Write(ctx, &m.Message)
		}
		// close any windows that need to be closed.
		wm := rl.waterMark(m)
		closedWindows := rl.aw.RemoveWindow(time.Time(wm))

		for _, cw := range closedWindows {
			partitions := cw.Partitions()
			rl.closePartitions(partitions)
		}
	}
}

func (rl *ReadLoop) processPartition(ctx context.Context, partitionID partition.ID) pbq.ReadWriteCloser {
	var q pbq.ReadWriteCloser
	// create or get existing pbq
	q = rl.pbqManager.GetPBQ(partitionID)

	if q == nil {
		var pbqErr error
		for {
			q, pbqErr = rl.pbqManager.CreateNewPBQ(ctx, partitionID)
			if pbqErr == nil {
				break
			}
			time.Sleep(retryDelay)
		}
		// if we did create a brand new PBQ it means this is a new partition
		// we should attach the read side of the loop to the partition
		// start process and forward loop here
		for {
			udf, err := function.NewUDSGRPCBasedUDF()
			if err == nil {
				rl.op.process(ctx, udf, q, partitionID)
				break
			}
			time.Sleep(retryDelay)
		}

	}
	return q
}

func (rl *ReadLoop) ShutDown(ctx context.Context) {
	rl.pbqManager.ShutDown(ctx)
}

func (rl *ReadLoop) upsertWindowsAndKeys(m *isb.ReadMessage) []*keyed.KeyedWindow {
	processingWindows := rl.windowingStrategy.AssignWindow(m.EventTime)
	var kWindows []*keyed.KeyedWindow
	for _, win := range processingWindows {
		kw := rl.aw.GetKeyedWindow(win)
		if kw == nil {
			kw = rl.aw.CreateKeyedWindow(win)
		}
		kw.AddKey(m.Key)
		kWindows = append(kWindows, kw)
	}
	return kWindows
}

func (rl *ReadLoop) waterMark(message *isb.ReadMessage) processor.Watermark {
	// TODO: change this to lookup watermark based on offset.
	return processor.Watermark(message.EventTime)
}

func (rl *ReadLoop) closePartitions(partitions []partition.ID) {
	for _, p := range partitions {
		q := rl.pbqManager.GetPBQ(p)
		q.CloseOfBook()
	}
}

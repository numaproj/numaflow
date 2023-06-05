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

package pnf

import (
	"container/list"
	"context"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

var retryDelay = 1 * time.Second

// ForwardTask wraps the `processAndForward`.
type ForwardTask struct {
	// doneCh is used to notify when the ForwardTask has been completed.
	doneCh chan struct{}
	pf     *processAndForward
}

// OrderedProcessor orders the forwarding of the writeMessages of the execution of the tasks, even though the tasks itself are
// run concurrently in an out of ordered fashion.
type OrderedProcessor struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	sync.RWMutex
	taskDone            chan struct{}
	taskQueue           *list.List
	pbqManager          *pbq.Manager
	udf                 applier.ReduceApplier
	toBuffers           map[string][]isb.BufferWriter
	whereToDecider      forward.ToWhichStepDecider
	watermarkPublishers map[string]publish.Publisher
	idleManager         *wmb.IdleManager
	log                 *zap.SugaredLogger
}

// NewOrderedProcessor returns an OrderedProcessor.
func NewOrderedProcessor(ctx context.Context,
	vertexInstance *dfv1.VertexInstance,
	udf applier.ReduceApplier,
	toBuffers map[string][]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forward.ToWhichStepDecider,
	watermarkPublishers map[string]publish.Publisher,
	idleManager *wmb.IdleManager) *OrderedProcessor {

	of := &OrderedProcessor{
		vertexName:          vertexInstance.Vertex.Spec.Name,
		pipelineName:        vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:       vertexInstance.Replica,
		taskDone:            make(chan struct{}),
		taskQueue:           list.New(),
		pbqManager:          pbqManager,
		udf:                 udf,
		toBuffers:           toBuffers,
		whereToDecider:      whereToDecider,
		watermarkPublishers: watermarkPublishers,
		idleManager:         idleManager,
		log:                 logging.FromContext(ctx),
	}

	go of.forward(ctx)

	return of
}

func (op *OrderedProcessor) InsertTask(t *ForwardTask) {
	op.Lock()
	defer op.Unlock()
	op.taskQueue.PushBack(t)
}

// SchedulePnF creates and schedules the PnF routine.
func (op *OrderedProcessor) SchedulePnF(
	ctx context.Context,
	partitionID partition.ID) *ForwardTask {

	pbq := op.pbqManager.GetPBQ(partitionID)

	pf := newProcessAndForward(ctx, op.vertexName, op.pipelineName, op.vertexReplica, partitionID, op.udf, pbq, op.toBuffers, op.whereToDecider, op.watermarkPublishers, op.idleManager)

	doneCh := make(chan struct{})
	t := &ForwardTask{
		doneCh: doneCh,
		pf:     pf,
	}
	partitionsInFlight.With(map[string]string{
		metrics.LabelVertex:             op.vertexName,
		metrics.LabelPipeline:           op.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(op.vertexReplica)),
	}).Inc()

	// invoke the reduce function
	go op.reduceOp(ctx, t)
	return t
}

// reduceOp invokes the reduce function. The reducer is a long-running function since we stream in the data and it has
// to wait for the close-of-book on the PBQ to materialize the writeMessages.
func (op *OrderedProcessor) reduceOp(ctx context.Context, t *ForwardTask) {
	start := time.Now()
	for {
		// FIXME: this error handling won't work with streams. We cannot do infinite retries
		//  because whatever is written to the stream is lost between retries.
		err := t.pf.Process(ctx)
		if err == nil {
			break
		} else if err == ctx.Err() {
			udfError.With(map[string]string{
				metrics.LabelVertex:             op.vertexName,
				metrics.LabelPipeline:           op.pipelineName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(op.vertexReplica)),
			}).Inc()
			op.log.Infow("ReduceOp exiting", zap.String("partitionID", t.pf.PartitionID.String()), zap.Error(ctx.Err()))
			return
		}
		op.log.Errorw("Process failed", zap.String("partitionID", t.pf.PartitionID.String()), zap.Error(err))
		time.Sleep(retryDelay)
	}

	// indicate that we are done with reduce UDF invocation.
	close(t.doneCh)
	op.log.Debugw("Process->Reduce call took ", zap.String("partitionID", t.pf.PartitionID.String()), zap.Int64("duration(ms)", time.Since(start).Milliseconds()))

	// notify that some work has been completed
	select {
	case op.taskDone <- struct{}{}:
	case <-ctx.Done():
		return
	}
}

// forward monitors the ForwardTask queue, as soon as the ForwardTask at the head of the queue has been completed, the writeMessages is
// forwarded to the next ISB. It keeps doing this for forever or until ctx.Done() happens.
func (op *OrderedProcessor) forward(ctx context.Context) {
	var currElement *list.Element
	var t *ForwardTask

outerLoop:
	for {
		start := time.Now()
		// block till we have some work
		select {
		case <-op.taskDone:
		case <-ctx.Done():
			op.log.Infow("forward exiting while waiting for ForwardTask completion event", zap.Error(ctx.Err()))
			break outerLoop
		}
		op.log.Debugw("Time waited for a completion event to happen ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))

		// a signal does not mean that we have any pending work to be done because
		// for every signal, we try to empty out the ForwardTask-queue.
		op.RLock()
		n := op.taskQueue.Len()
		op.log.Debugw("Received a signal in ForwardTask queue ", zap.Int("ForwardTask count", n))

		op.RUnlock()
		// n could be 0 because we have emptied the queue
		if n == 0 {
			continue
		}

		// now that we know there is at least an element, let's start from the front.
		op.RLock()
		currElement = op.taskQueue.Front()
		op.RUnlock()

		// empty out the entire ForwardTask-queue everytime there has been some work done
		startLoop := time.Now()
		for i := 0; i < n; i++ {
			t = currElement.Value.(*ForwardTask)
			select {
			case <-t.doneCh:
				for {
					err := t.pf.Forward(ctx)
					if err != nil {
						logging.FromContext(ctx).Error(err)
						time.Sleep(retryDelay)
					} else {
						break
					}
				}
				op.Lock()
				rm := currElement
				currElement = currElement.Next()
				op.taskQueue.Remove(rm)
				partitionsInFlight.With(map[string]string{
					metrics.LabelVertex:             op.vertexName,
					metrics.LabelPipeline:           op.pipelineName,
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(op.vertexReplica)),
				}).Dec()

				op.log.Debugw("Removing ForwardTask post forward call", zap.String("partitionID", t.pf.PartitionID.String()))
				op.Unlock()
			case <-ctx.Done():
				op.log.Infow("Forward exiting while waiting on the head op the queue ForwardTask", zap.String("partitionID", t.pf.PartitionID.String()), zap.Error(ctx.Err()))
				break outerLoop
			}
		}
		op.log.Debugw("One iteration op the ordered tasks queue loop took ", zap.Int64("duration(ms)", time.Since(startLoop).Milliseconds()), zap.Int("elements", n))
	}
	op.Shutdown()
}

// Shutdown closes all the partitions of the buffer.
func (op *OrderedProcessor) Shutdown() {
	for _, buffer := range op.toBuffers {
		for _, partition := range buffer {
			if err := partition.Close(); err != nil {
				op.log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", partition.GetName()))
			} else {
				op.log.Infow("Closed partition writer", zap.String("bufferTo", partition.GetName()))
			}
		}
	}
}

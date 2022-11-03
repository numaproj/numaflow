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

package readloop

import (
	"container/list"
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/watermark/publish"

	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
)

var retryDelay = 1 * time.Second

// task wraps the `ProcessAndForward`.
type task struct {
	// doneCh is used to notify when the task has been completed.
	doneCh chan struct{}
	pf     *pnf.ProcessAndForward
}

// orderedForwarder orders the forwarding of the result of the execution of the tasks, even though the tasks itself are
// run concurrently in an out of ordered fashion.
type orderedForwarder struct {
	sync.RWMutex
	taskDone  chan struct{}
	taskQueue *list.List
	log       *zap.SugaredLogger
}

// newOrderedForwarder returns an orderedForwarder.
func newOrderedForwarder(ctx context.Context) *orderedForwarder {
	return &orderedForwarder{
		taskDone:  make(chan struct{}),
		taskQueue: list.New(),
		log:       logging.FromContext(ctx),
	}
}

// startUp starts forwarder.
func (of *orderedForwarder) startUp(ctx context.Context) {
	go of.forward(ctx)
}

// schedulePnF creates and schedules the PnF routine.
func (of *orderedForwarder) schedulePnF(ctx context.Context,
	udf udfreducer.Reducer,
	pbq pbq.Reader,
	partitionID partition.ID,
	toBuffers map[string]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider,
	pw map[string]publish.Publisher) {

	pf := pnf.NewProcessAndForward(ctx, partitionID, udf, pbq, toBuffers, whereToDecider, pw)
	doneCh := make(chan struct{})
	t := &task{
		doneCh: doneCh,
		pf:     pf,
	}

	of.Lock()
	defer of.Unlock()
	of.taskQueue.PushBack(t)

	// invoke the reduce function
	go of.reduceOp(ctx, t)
}

// reduceOp invokes the reduce function. The reducer is a long running function since we stream in the data and it has
// to wait for the close-of-book on the PBQ to materialize the result.
func (of *orderedForwarder) reduceOp(ctx context.Context, t *task) {
	// TODO: better logging with partitionID?
	start := time.Now()
	for {
		// FIXME: this error handling won't work with streams. We cannot do infinite retries
		//  because whatever is written to the stream is lost between retries.
		err := t.pf.Process(ctx)
		if err == nil {
			break
		} else if err == ctx.Err() {
			of.log.Infow("ReduceOp exiting", zap.Error(ctx.Err()))
			return
		}
		of.log.Errorw("Process failed", zap.Error(err))
		time.Sleep(retryDelay)
	}
	// after retrying indicate that we are done with processing the package. the processing can move on
	close(t.doneCh)
	of.log.Debugw("Process->Reduce call took ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))
	// notify that some work has been completed
	select {
	case of.taskDone <- struct{}{}:
	case <-ctx.Done():
		return
	}

	// TODO: remove this?
	of.log.Debugw("Post to task done chan took ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))
}

// forward monitors the task queue, as soon as the task at the head of the queue has been completed, the result is
// forwarded to the next ISB. It keeps doing this for forever or until ctx.Done() happens.
func (of *orderedForwarder) forward(ctx context.Context) {
	var currElement *list.Element
	var t *task

	for {
		start := time.Now()
		// block till we have some work
		select {
		case <-of.taskDone:
		case <-ctx.Done():
			of.log.Infow("Forward exiting while waiting for task completion event", zap.Error(ctx.Err()))
			return
		}
		of.log.Debugw("Time waited for a completion event to happen ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))

		// a signal does not mean that we have any pending work to be done because
		// for every signal, we try to empty out the task-queue.
		of.RLock()
		n := of.taskQueue.Len()
		of.RUnlock()
		// n could be 0 because we have emptied the queue
		if n == 0 {
			continue
		}

		// now that we know there is at least an element, let's start from the front.
		of.RLock()
		currElement = of.taskQueue.Front()
		of.RUnlock()

		// empty out the entire task-queue everytime there has been some work done
		startLoop := time.Now()
		for i := 0; i < n; i++ {
			t = currElement.Value.(*task)
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
				of.Lock()
				rm := currElement
				currElement = currElement.Next()
				of.taskQueue.Remove(rm)
				of.Unlock()
			case <-ctx.Done():
				of.log.Infow("Forward exiting while waiting on the head of the queue task", zap.Error(ctx.Err()))
				return
			}
		}
		of.log.Debugw("One iteration of the ordered tasks queue loop took ", zap.Int64("duration(ms)", time.Since(startLoop).Milliseconds()), zap.Int("elements", n))
	}
}

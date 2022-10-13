package readloop

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/watermark/publish"

	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
)

type task struct {
	doneCh chan struct{}
	pf     *pnf.ProcessAndForward
}

// orderedProcessor orders the execution of the tasks, even though the tasks itself are run concurrently.
type orderedProcessor struct {
	sync.RWMutex
	hasWork   chan struct{}
	taskQueue *list.List
}

// newOrderedProcessor returns an orderedProcessor.
func newOrderedProcessor() *orderedProcessor {
	return &orderedProcessor{
		hasWork:   make(chan struct{}),
		taskQueue: list.New(),
	}
}

func (op *orderedProcessor) startUp(ctx context.Context) {
	go op.forward(ctx)
}

func (op *orderedProcessor) process(ctx context.Context,
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

	op.Lock()
	defer op.Unlock()
	op.taskQueue.PushBack(t)

	// invoke the reduce function
	go op.reduceOp(ctx, t)
}

// reduceOp invokes the reduce function. The reducer is a long running function since we stream in the data and it has
// to wait for the close-of-book on the PBQ to materialize the result.
func (op *orderedProcessor) reduceOp(ctx context.Context, t *task) {
	for {
		// FIXME: this error handling won't work with streams. We cannot do infinite retries
		//  because whatever is written to the stream is lost between retries.
		err := t.pf.Process(ctx)
		if err == nil {
			break
		} else if err == ctx.Err() {
			return
		}

		logging.FromContext(ctx).Error(err)
		time.Sleep(retryDelay)
	}
	// after retrying indicate that we are done with processing the package. the processing can move on
	close(t.doneCh)

	// notify that some work has been completed
	select {
	case op.hasWork <- struct{}{}:
	case <-ctx.Done():
		return
	}

}

func (op *orderedProcessor) forward(ctx context.Context) {
	var currElement *list.Element
	var t *task
	for {
		// block till we have some work
		select {
		case <-op.hasWork:
		case <-ctx.Done():
			return
		}

		// a signal does not mean we have pending work to be done because
		// for every signal we try to empty out the task-queue.
		op.RLock()
		n := op.taskQueue.Len()
		op.RUnlock()
		// n could we 0 because we have emptied the queue
		if n == 0 {
			continue
		}

		// now that we know there is at least an element, let's start from the front.
		op.RLock()
		currElement = op.taskQueue.Front()
		op.RUnlock()

		// empty out the entire task-queue everytime there has been some work done
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
				op.Lock()
				rm := currElement
				currElement = currElement.Next()
				op.taskQueue.Remove(rm)
				op.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}
}

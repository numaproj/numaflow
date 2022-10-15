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

type task struct {
	doneCh chan struct{}
	pf     *pnf.ProcessAndForward
}

// orderedProcessor orders the execution of the tasks, even though the tasks itself are run concurrently.
type orderedProcessor struct {
	sync.RWMutex
	hasWork   chan struct{}
	taskQueue *list.List
	log       *zap.SugaredLogger
}

// newOrderedProcessor returns an orderedProcessor.
func newOrderedProcessor(ctx context.Context) *orderedProcessor {
	return &orderedProcessor{
		hasWork:   make(chan struct{}),
		taskQueue: list.New(),
		log:       logging.FromContext(ctx),
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
	start := time.Now()
	for {
		// FIXME: this error handling won't work with streams. We cannot do infinite retries
		//  because whatever is written to the stream is lost between retries.
		err := t.pf.Process(ctx)
		if err == nil {
			break
		} else if err == ctx.Err() {
			logging.FromContext(ctx).Infow("ReduceOp returning early", zap.Error(ctx.Err()))
			return
		}

		op.log.Error(err)
		time.Sleep(retryDelay)
	}
	// after retrying indicate that we are done with processing the package. the processing can move on
	close(t.doneCh)
	op.log.Debugw("Process->Reduce call took ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))
	// notify that some work has been completed
	select {
	case op.hasWork <- struct{}{}:
	case <-ctx.Done():
		return
	}

	op.log.Debugw("Post Reduce chan push took ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))
}

func (op *orderedProcessor) forward(ctx context.Context) {

	var currElement *list.Element
	var t *task
	for {
		start := time.Now()
		// block till we have some work
		select {
		case <-op.hasWork:
		case <-ctx.Done():
			op.log.Infow("forward returning early", zap.Error(ctx.Err()))
			return
		}
		op.log.Debugw("waited for dequeuing tasks ", zap.Int64("duration(ms)", time.Since(start).Milliseconds()))

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
			startLoop := time.Now()
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
				logging.FromContext(ctx).Debugw("forward returning early", zap.Error(ctx.Err()))
				return
			}
			op.log.Debugw("one iteration of ordered loop took ", zap.Int64("duration(ms)", time.Since(startLoop).Milliseconds()))

		}
	}
}

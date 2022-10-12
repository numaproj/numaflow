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
	taskQueue *list.List
}

// newOrderedProcessor returns an orderedProcessor.
func newOrderedProcessor() *orderedProcessor {
	return &orderedProcessor{
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
		err := t.pf.Process(ctx)
		if err == nil || err == ctx.Err() {
			break
		}

		logging.FromContext(ctx).Error(err)
		time.Sleep(retryDelay)
	}
	// after retrying indicate that we are done with processing the package. the processing can move on
	close(t.doneCh)
}

func (op *orderedProcessor) forward(ctx context.Context) {
	var currElement *list.Element
	var t *task
	for {
		op.RLock()
		isEmpty := op.taskQueue.Len() == 0
		op.RUnlock()
		if isEmpty {
			continue
		}
		if currElement == nil {
			op.RLock()
			currElement = op.taskQueue.Front()
			op.RUnlock()
		}
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
			break
		case <-ctx.Done():
			return
		}
	}
}

package readloop

import (
	"container/list"
	"context"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pandf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
)

type task struct {
	doneCh chan struct{}
	pf     *pandf.ProcessAndForward
}

type orderedProcessor struct {
	sync.RWMutex
	taskQueue *list.List
}

func NewOrderedProcessor() *orderedProcessor {
	return &orderedProcessor{
		taskQueue: list.New(),
	}
}

func (op *orderedProcessor) StartUp(ctx context.Context) {
	go op.forward(ctx)
}

func (op *orderedProcessor) process(ctx context.Context, udf udfreducer.Reducer, pbq pbq.Reader, partitionID partition.ID) {
	pf := pandf.NewProcessAndForward(partitionID, udf, pbq)
	doneCh := make(chan struct{})
	t := &task{
		doneCh: doneCh,
		pf:     pf,
	}
	op.Lock()
	defer op.Unlock()
	op.taskQueue.PushBack(t)
	go op.reduceOp(ctx, t)
}

func (op *orderedProcessor) reduceOp(ctx context.Context, t *task) {
	for {
		err := t.pf.Process(ctx)
		if err == ctx.Err() || err == nil {
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
				// TODO implement forward with WhereTo and toStep Buffers
				err := t.pf.Forward()
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

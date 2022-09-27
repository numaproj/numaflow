package readloop

import (
	"container/list"
	"context"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pandf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"time"
)

type task struct {
	doneCh chan struct{}
	pf     *pandf.ProcessAndForward
}

type orderedProcessor struct {
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

func (op *orderedProcessor) process(ctx context.Context, udf udfreducer.Reducer, pbq pbq.Reader, key keyed.PartitionId) {

	pf := pandf.NewProcessAndForward(string(key), udf, pbq)
	doneCh := make(chan struct{})
	t := &task{
		doneCh: doneCh,
		pf:     pf,
	}
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
		if op.taskQueue.Len() == 0 {
			continue
		}
		if currElement == nil {
			currElement = op.taskQueue.Front()
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
			rm := currElement
			currElement = currElement.Next()
			op.taskQueue.Remove(rm)
			break
		case <-ctx.Done():
			return
		}
	}
}

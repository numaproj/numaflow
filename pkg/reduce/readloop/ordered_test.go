package readloop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/udf/reducer"
	"github.com/stretchr/testify/assert"
)

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ string) ([]string, error) {
	return []string{dfv1.MessageKeyDrop}, nil
}

func (f myForwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func TestOrderedProcessing(t *testing.T) {

	// Test Reducer returns the messages as is
	identityReducer := reducer.ReduceFunc(func(ctx context.Context, partitionID *partition.ID, input <-chan *isb.ReadMessage) ([]*isb.Message, error) {
		messages := make([]*isb.Message, 0)
		for msg := range input {
			messages = append(messages, &msg.Message)
		}
		return messages, nil
	})
	to1 := simplebuffer.NewInMemoryBuffer("to1", 100)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	_, pw := generic.BuildNoOpWatermarkProgressorsFromBufferMap(make(map[string]isb.BufferWriter))

	ctx := context.Background()

	tests := []struct {
		name           string
		partitions     []partition.ID
		reduceOrder    []partition.ID
		expectedBefore int
		expectedAfter  []partition.ID
	}{
		{
			name:           "single-task-finished",
			partitions:     partitions(1),
			expectedBefore: 1,
			reduceOrder:    partitionsFor([]int{0}),
			expectedAfter:  []partition.ID{},
		},
		{
			name:           "middle-task-finished-in-multiple-tasks",
			partitions:     partitions(4),
			expectedBefore: 4,
			reduceOrder:    partitionsFor([]int{2}),
			expectedAfter:  partitionsFor([]int{0, 1, 2, 3}),
		},
		{
			name:           "tasks-in-order",
			partitions:     partitions(4),
			expectedBefore: 4,
			reduceOrder:    partitionsFor([]int{0, 1, 2, 3}),
			expectedAfter:  partitionsFor([]int{}),
		},
		{
			name:           "first-task-multiple-tasks",
			partitions:     partitions(4),
			expectedBefore: 4,
			reduceOrder:    partitionsFor([]int{0}),
			expectedAfter:  partitionsFor([]int{1, 2, 3}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// clean out the task queue before we start a run
			op := newOrderedForwarder(ctx)
			op.startUp(ctx)
			// although this could be declared outside, since we are using common naming scheme for partitions,
			// things will go haywire.
			pbqManager, _ := pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(int64(100)), store.WithPbqStoreType(dfv1.InMemoryType)),
				pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
			cCtx, cancelFn := context.WithCancel(ctx)
			defer cancelFn()
			for _, _partition := range tt.partitions {
				p, _ := pbqManager.CreateNewPBQ(ctx, _partition)
				op.schedulePnF(cCtx, identityReducer, p, _partition, toSteps, myForwardTest{}, pw)
			}
			assert.Equal(t, op.taskQueue.Len(), tt.expectedBefore)
			count := 0
			for e := op.taskQueue.Front(); e != nil; e = e.Next() {
				pfTask := e.Value.(*task)
				partitionKey := pfTask.pf.PartitionID
				assert.Equal(t, partition.ID{Key: fmt.Sprintf("partition-%d", count)}, partitionKey)
				count = count + 1
			}

			for _, id := range tt.reduceOrder {
				p := pbqManager.GetPBQ(id)
				p.CloseOfBook()
				// wait for reduce operation to be done.
				pfTask := taskForPartition(op, id)
				// it might so happen that this task got processed and removed
				// before we could even inspect it.
				if pfTask == nil {
					continue
				}
				<-pfTask.doneCh
			}

			if len(tt.expectedAfter) == 0 {
				time.Sleep(100 * time.Millisecond)
			}

			for _, partitionId := range tt.expectedAfter {
				pfTask := taskForPartition(op, partitionId)
				assert.NotNil(t, pfTask)
			}

		})
	}

}

func partitions(count int) []partition.ID {
	partitions := make([]partition.ID, count)
	for i := 0; i < count; i++ {
		partitions[i] = partition.ID{Key: fmt.Sprintf("partition-%d", i)}
	}
	return partitions
}

func partitionsFor(partitionIdx []int) []partition.ID {
	partitions := make([]partition.ID, len(partitionIdx))
	for i, idx := range partitionIdx {
		partitions[i] = partition.ID{Key: fmt.Sprintf("partition-%d", idx)}
	}
	return partitions
}

func taskForPartition(op *orderedForwarder, partitionId partition.ID) *task {
	op.RLock()
	defer op.RUnlock()
	for e := op.taskQueue.Front(); e != nil; e = e.Next() {
		pfTask := e.Value.(*task)
		partitionKey := pfTask.pf.PartitionID
		if partitionKey == partitionId {
			return pfTask
		}
	}
	return nil
}

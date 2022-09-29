package readloop

import (
	"context"
	"fmt"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/udf/reducer"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/stretchr/testify/assert"
)

func TestOrderedProcessing(t *testing.T) {

	// Test Reducer returns the messages as is
	identityReducer := reducer.ReduceFunc(func(ctx context.Context, input <-chan *isb.Message) ([]*isb.Message, error) {
		messages := make([]*isb.Message, 0)
		for msg := range input {
			messages = append(messages, msg)
		}
		return messages, nil
	})

	ctx := context.Background()

	tests := []struct {
		name           string
		partitions     []keyed.PartitionID
		reduceOrder    []keyed.PartitionID
		expectedBefore int
		expectedAfter  []keyed.PartitionID
	}{
		{
			name:           "single-task-finished",
			partitions:     partitions(1),
			expectedBefore: 1,
			reduceOrder:    partitionsFor([]int{0}),
			expectedAfter:  []keyed.PartitionID{},
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
			op := NewOrderedProcessor()
			op.StartUp(ctx)
			// although this could be declared outside, since we are using common naming scheme for partitions,
			// things will go haywire.
			pbqManager, _ := pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(int64(100)), store.WithPbqStoreType(dfv1.InMemoryType)),
				pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
			cCtx, _ := context.WithCancel(ctx)
			for _, partition := range tt.partitions {
				p, _ := pbqManager.CreateNewPBQ(ctx, partition)
				op.process(cCtx, identityReducer, p, partition)
			}
			assert.Equal(t, op.taskQueue.Len(), tt.expectedBefore)
			count := 0
			for e := op.taskQueue.Front(); e != nil; e = e.Next() {
				pfTask := e.Value.(*task)
				partitionKey := pfTask.pf.Key
				assert.Equal(t, keyed.PartitionID{Key: fmt.Sprintf("partition-%d", count)}, partitionKey)
				count = count + 1
			}

			for _, id := range tt.reduceOrder {
				p := pbqManager.GetPBQ(id)
				p.CloseOfBook()
				// wait for reduce operation to be done.
				pfTask := taskForPartition(op, id)
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

func partitions(count int) []keyed.PartitionID {
	partitions := make([]keyed.PartitionID, count)
	for i := 0; i < count; i++ {
		partitions[i] = keyed.PartitionID{Key: fmt.Sprintf("partition-%d", i)}
	}
	return partitions
}

func partitionsFor(partitionIdx []int) []keyed.PartitionID {
	partitions := make([]keyed.PartitionID, len(partitionIdx))
	for i, idx := range partitionIdx {
		partitions[i] = keyed.PartitionID{Key: fmt.Sprintf("partition-%d", idx)}
	}
	return partitions
}

func taskForPartition(op *orderedProcessor, partitionId keyed.PartitionID) *task {
	op.RLock()
	defer op.RUnlock()
	for e := op.taskQueue.Front(); e != nil; e = e.Next() {
		pfTask := e.Value.(*task)
		partitionKey := pfTask.pf.Key
		if partitionKey == partitionId {
			return pfTask
		}
	}
	return nil
}

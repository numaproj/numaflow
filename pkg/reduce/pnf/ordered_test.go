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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{}, nil
}

func (f myForwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

var keyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			UDF:  &dfv1.UDF{GroupBy: &dfv1.GroupBy{Keyed: true}},
		},
	}},
	Hostname: "test-host",
	Replica:  0,
}

func TestOrderedProcessing(t *testing.T) {
	// Test Reducer returns the messages as is
	identityReducer := applier.ApplyReduceFunc(func(ctx context.Context, partitionID *partition.ID, input <-chan *isb.ReadMessage) ([]*isb.WriteMessage, error) {
		messages := make([]*isb.WriteMessage, 0)
		for msg := range input {
			messages = append(messages, &isb.WriteMessage{Message: msg.Message})
		}
		return messages, nil
	})
	to1 := simplebuffer.NewInMemoryBuffer("to1", 100, 0)
	toSteps := map[string][]isb.BufferWriter{
		"to1": {to1},
	}

	idleManager := wmb.NewIdleManager(len(toSteps))
	_, pw := generic.BuildNoOpWatermarkProgressorsFromBufferMap(make(map[string][]isb.BufferWriter))

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
			// although this could be declared outside, since we are using common naming scheme for partitions,
			// things will go haywire.
			pbqManager, _ := pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(100)),
				pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))

			op := NewOrderedProcessor(ctx, keyedVertex, identityReducer, toSteps, pbqManager, myForwardTest{}, pw, idleManager)
			cCtx, cancelFn := context.WithCancel(ctx)
			defer cancelFn()
			for _, _partition := range tt.partitions {
				kw := keyed.NewKeyedWindow(_partition.Start, _partition.End)
				kw.AddSlot(_partition.Slot)

				_, _ = pbqManager.CreateNewPBQ(ctx, _partition)
				t := op.SchedulePnF(cCtx, _partition)
				op.InsertTask(t)
			}
			assert.Equal(t, op.taskQueue.Len(), tt.expectedBefore)
			count := 0
			for e := op.taskQueue.Front(); e != nil; e = e.Next() {
				pfTask := e.Value.(*ForwardTask)
				assert.Equal(t, partitionFor(count), pfTask.pf.PartitionID)
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
		partitions[i] = partitionFor(i)
	}
	return partitions
}

func partitionsFor(partitionIdx []int) []partition.ID {
	partitions := make([]partition.ID, len(partitionIdx))
	for i, idx := range partitionIdx {
		partitions[i] = partitionFor(idx)
	}
	return partitions
}

func partitionFor(i int) partition.ID {
	base := 10000
	return partition.ID{Start: time.UnixMilli(int64(base * i)), End: time.UnixMilli(int64(base * (i + 1)))}
}

func taskForPartition(op *OrderedProcessor, partitionId partition.ID) *ForwardTask {
	op.RLock()
	defer op.RUnlock()
	for e := op.taskQueue.Front(); e != nil; e = e.Next() {
		pfTask := e.Value.(*ForwardTask)
		partitionKey := pfTask.pf.PartitionID
		if partitionKey == partitionId {
			return pfTask
		}
	}
	return nil
}

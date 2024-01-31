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
package pbq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned/memory"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned/noop"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestManager_ListPartitions(t *testing.T) {
	size := int64(100)

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(size)),
		window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using pbq manager
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}
	partitionTwo := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-2",
	}

	var pq1, pq2 ReadWriteCloser
	pq1, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	pq2, err = pbqManager.CreateNewPBQ(ctx, partitionTwo)
	assert.NoError(t, err)

	assert.Len(t, pbqManager.ListPartitions(), 2)

	err = pq1.GC()
	assert.NoError(t, err)
	err = pq2.GC()
	assert.NoError(t, err)

	// after deregister is called, entry in the map should be deleted
	assert.Len(t, pbqManager.ListPartitions(), 0)

}

func TestManager_GetPBQ(t *testing.T) {
	size := int64(100)
	var pb1, pb2 ReadWriteCloser
	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(size)),
		window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using CreateNewPBQ PBQ
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	pb1, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	// get the created pbq
	pb2 = pbqManager.GetPBQ(testPartition)

	assert.Equal(t, pb1, pb2)
}

// manager -> pbq -> store
func TestPBQFlow(t *testing.T) {
	size := int64(100)

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(size)),
		window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	msgsCount := 5
	var wg sync.WaitGroup
	wg.Add(1)

	// window requests from pbq
	var windowRequests []*window.TimedWindowRequest

	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					windowRequests = append(windowRequests, msg)
				}
				if !ok {
					break readLoop
				}
			case <-ctx.Done():
				break readLoop
			}
		}
		wg.Done()
	}()

	readRequests := testutils.BuildTestWindowRequests(int64(msgsCount), time.Now(), window.Append)
	for _, msg := range readRequests {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()
	assert.NoError(t, err)

	wg.Wait()
	// check if we are able to read all the requests
	assert.Len(t, windowRequests, len(readRequests))

	err = pq.GC()
	assert.NoError(t, err)
}

func TestPBQFlowWithNoOpStore(t *testing.T) {

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, noop.NewNoopStores(),
		window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	// create a pbq backed with no op store
	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	msgsCount := 50
	var wg sync.WaitGroup

	var windowRequests []*window.TimedWindowRequest

	// read requests from pbq(with no op store)
	wg.Add(1)
	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					windowRequests = append(windowRequests, msg)
				}
				if !ok {
					break readLoop
				}
			case <-ctx.Done():
				break readLoop
			}
		}
		wg.Done()
	}()

	requests := testutils.BuildTestWindowRequests(int64(msgsCount), time.Now(), window.Append)
	for _, msg := range requests {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}
	pbqManager.Replay(ctx)

	pq.CloseOfBook()
	assert.NoError(t, err)

	wg.Wait()
	// since it's a no-op store, there should no messages to reply
	// no of windowRequests will be equal to no of produced messages
	assert.Len(t, windowRequests, msgsCount)
}

func TestManager_Replay(t *testing.T) {
	size := int64(100)

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(size)), window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10), WithReadBatchSize(10))
	assert.NoError(t, err)
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)

	var readRequests []*window.TimedWindowRequest
	// go routine which reads the requests from pbq
	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					readRequests = append(readRequests, msg)
				}
				if !ok {
					break readLoop
				}
			case <-ctx.Done():
				break readLoop
			}
		}
		wg.Done()
	}()

	// write 50 requests to pbq
	msgsCount := 50
	writeRequests := testutils.BuildTestWindowRequests(int64(msgsCount), time.Now(), window.Append)

	for _, msg := range writeRequests {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}

	// after writing, replay records from the store using Manager
	pbqManager.Replay(ctx)

	pq.CloseOfBook()
	wg.Wait()
	// number of read requests should be twice compared to number of requests produced
	// since we replayed the messages which are persisted in store
	assert.Len(t, readRequests, 2*msgsCount)
}

func TestManager_StartUp(t *testing.T) {
	size := int64(100)

	pID1 := partition.ID{
		Start: time.Now(),
		End:   time.Now(),
		Slot:  "test-partition-1",
	}

	pID2 := partition.ID{
		Start: time.Now(),
		End:   time.Now(),
		Slot:  "test-partition-2",
	}
	dp := func(context.Context) ([]partition.ID, error) {
		return []partition.ID{pID1,
			pID2}, nil
	}
	stores := memory.NewMemoryStores(memory.WithStoreSize(size), memory.WithDiscoverer(dp))
	ctx := context.Background()
	pbqManager, err := NewManager(ctx, "reduce", "test-pipeline", 0, stores, window.Aligned, WithReadTimeout(1*time.Second), WithChannelBufferSize(10), WithReadBatchSize(10))
	assert.NoError(t, err)
	ids, err := pbqManager.GetExistingPartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, ids, 2)
}

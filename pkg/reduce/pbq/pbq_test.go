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
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/window"
)

// test cases for PBQ (store type in-memory)

func TestPBQ_ReadWrite(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := int64(100)
	// create a pbq with buffer size 5
	buffSize := 5

	ctx := context.Background()

	qManager, _ := NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(storeSize)),
		window.Fixed, WithChannelBufferSize(int64(buffSize)), WithReadTimeout(1*time.Second))

	// write 10 window requests
	count := 10
	startTime := time.Now()
	writeRequests := testutils.BuildTestWindowRequests(int64(count), startTime, window.Append)

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	pq, err := qManager.CreateNewPBQ(ctx, partitionID)
	assert.NoError(t, err)

	var readRequests []*window.TimedWindowRequest
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

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

	for _, msg := range writeRequests {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}
	pq.CloseOfBook()

	wg.Wait()
	// count of requests read by parallel go routine should be equal to produced messages
	assert.Len(t, readRequests, count)

}

func Test_PBQReadWithCanceledContext(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := int64(100)
	//create a pbq with buffer size 10
	bufferSize := 10
	var err error
	var qManager *Manager

	ctx := context.Background()

	qManager, err = NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(storeSize)),
		window.Fixed, WithChannelBufferSize(int64(bufferSize)), WithReadTimeout(1*time.Second))

	assert.NoError(t, err)

	//write 10 window requests to persisted store
	count := 10
	startTime := time.Now()
	windowRequests := testutils.BuildTestWindowRequests(int64(count), startTime, window.Append)

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	var pq ReadWriteCloser
	pq, err = qManager.CreateNewPBQ(ctx, partitionID)
	assert.NoError(t, err)

	var readRequests []*window.TimedWindowRequest
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

	childCtx, cancelFn := context.WithCancel(ctx)

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
			case <-childCtx.Done():
				break readLoop
			}
		}
		wg.Done()
	}()

	for _, msg := range windowRequests {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)
	//since we are closing the context, read should exit
	cancelFn()

	wg.Wait()
	assert.Len(t, readRequests, 10)
}

func TestPBQ_WriteWithStoreFull(t *testing.T) {

	// create a store of size 100 (it can store max 100 messages)
	storeSize := int64(100)
	// create a pbq with buffer size 101
	buffSize := 101
	var qManager *Manager
	var err error
	ctx := context.Background()

	qManager, err = NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(storeSize)),
		window.Fixed, WithChannelBufferSize(int64(buffSize)), WithReadTimeout(1*time.Second))
	assert.NoError(t, err)

	// write 101 window requests to pbq, but the store size is 100, we should get store is full error
	msgCount := 101
	startTime := time.Now()
	windowRequests := testutils.BuildTestWindowRequests(int64(msgCount), startTime, window.Append)
	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "slot-1",
	}

	var pq ReadWriteCloser
	pq, err = qManager.CreateNewPBQ(ctx, partitionID)
	assert.NoError(t, err)

	for _, req := range windowRequests {
		err = pq.Write(ctx, &req)
	}
	pq.CloseOfBook()

	assert.Error(t, err, store.WriteStoreFullErr)
}

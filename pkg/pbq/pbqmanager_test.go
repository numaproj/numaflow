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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
	"github.com/stretchr/testify/assert"
)

// tests for pbqManager (store type - in-memory)

func TestManager_ListPartitions(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using pbq manager
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-1",
	}
	partitionTwo := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-2",
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
	size := 100
	var pb1, pb2 ReadWriteCloser
	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using CreateNewPBQ PBQ
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-1",
	}
	pb1, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	// get the created pbq
	pb2 = pbqManager.GetPBQ(testPartition)

	assert.Equal(t, pb1, pb2)
}

// manager -> pbq -> store
func TestPBQFlow(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-1",
	}

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	msgsCount := 5
	var wg sync.WaitGroup
	wg.Add(1)

	// read messages from pbq
	var readMessages []*isb.ReadMessage

	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					readMessages = append(readMessages, msg)
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

	// write messages to pbq
	writeMessages := testutils.BuildTestReadMessages(int64(msgsCount), time.Now())
	for _, msg := range writeMessages {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()
	assert.NoError(t, err)

	wg.Wait()
	// check if we are able to read all the messages
	assert.Len(t, readMessages, len(writeMessages))

	err = pq.GC()
	assert.NoError(t, err)

}

func TestPBQFlowWithNoOpStore(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.NoOpType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-1",
	}

	// create a pbq backed with no op store
	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	msgsCount := 50
	var wg sync.WaitGroup

	var readMessages []*isb.ReadMessage

	// read messages from pbq(with no op store)
	wg.Add(1)
	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					readMessages = append(readMessages, msg)
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

	// write messages to pbq
	writeMessages := testutils.BuildTestReadMessages(int64(msgsCount), time.Now())
	for _, msg := range writeMessages {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}
	pbqManager.Replay(ctx)

	pq.CloseOfBook()
	assert.NoError(t, err)

	wg.Wait()
	// since it's a no-op store, there should no messages to reply
	// no of readMessages will be equal to no of produced messages
	assert.Len(t, readMessages, msgsCount)
}

func TestManager_Replay(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10), WithReadBatchSize(10))
	assert.NoError(t, err)
	testPartition := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Key:   "partition-1",
	}

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)

	var readMessages []*isb.ReadMessage
	// go routine which reads the messages from pbq
	go func() {
	readLoop:
		for {
			select {
			case msg, ok := <-pq.ReadCh():
				if msg != nil {
					readMessages = append(readMessages, msg)
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

	// write 50 messages to pbq
	msgsCount := 50
	writeMessages := testutils.BuildTestReadMessages(int64(msgsCount), time.Now())

	for _, msg := range writeMessages {
		err := pq.Write(ctx, &msg)
		assert.NoError(t, err)
	}

	// after writing, replay records from the store using Manager
	pbqManager.Replay(ctx)

	pq.CloseOfBook()
	wg.Wait()
	// number of read messages should be twice compared to number of messages produced
	// since we replayed the messages which are persisted in store
	assert.Len(t, readMessages, 2*msgsCount)
}

func TestManager_StartUp(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10), WithReadBatchSize(10))
	assert.NoError(t, err)
	pID1 := partition.ID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "test-partition-1",
	}

	pID2 := partition.ID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "test-partition-2",
	}
	dp := func(storeOptions *store.StoreOptions) ([]partition.ID, error) {
		return []partition.ID{pID1,
			pID2}, nil
	}
	memory.SetDiscoverer(dp)
	pbqManager.StartUp(ctx)
	assert.Len(t, pbqManager.ListPartitions(), 2)
	assert.NotNil(t, pbqManager.GetPBQ(pID1))
	assert.NotNil(t, pbqManager.GetPBQ(pID2))
}

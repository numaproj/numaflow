package pbq

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestManager_ListPartitions(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, store.WithStoreSize(int64(size)), store.WithPbqStoreType("in-memory"), store.WithReadTimeout(1), store.WithBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using pbq manager
	pq1, _, err := pbqManager.GetPBQ(ctx, "partition-1", true, dfv1.InMemoryStoreType)

	assert.NoError(t, err)

	pq2, _, err := pbqManager.GetPBQ(ctx, "partition-2", true, dfv1.InMemoryStoreType)
	assert.NoError(t, err)

	// list partitions should return 2 pbq entries
	for _, pq := range pbqManager.ListPartitions() {
		if pq.partitionID == "partition-1" {
			assert.Equal(t, pq, pq1)
		}
		if pq.partitionID == "partition-2" {
			assert.Equal(t, pq, pq2)
		}
	}

	err = pq1.GC()
	assert.NoError(t, err)
	err = pq2.GC()
	assert.NoError(t, err)

}

func TestManager_GetPBQ(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, store.WithStoreSize(int64(size)), store.WithPbqStoreType("in-memory"), store.WithReadTimeout(1), store.WithBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using Get PBQ
	pb1, _, err := pbqManager.GetPBQ(ctx, "partition-3", true, dfv1.InMemoryStoreType)
	assert.NoError(t, err)

	// get the created pbq
	pb2, _, err := pbqManager.GetPBQ(ctx, "partition-3", false, dfv1.InMemoryStoreType)

	assert.Equal(t, pb1, pb2)
}

// manager -> pbq -> store
func TestPBQFlow(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, store.WithStoreSize(int64(size)), store.WithPbqStoreType("in-memory"), store.WithReadTimeout(1), store.WithBufferSize(10))
	assert.NoError(t, err)

	pq, _, err := pbqManager.GetPBQ(ctx, "partition-4", true, dfv1.InMemoryStoreType)
	assert.NoError(t, err)
	msgsCount := 5
	var wg sync.WaitGroup
	wg.Add(2)

	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())

	go func() {
		for _, msg := range writeMessages {
			err := pq.WriteFromISB(&msg)
			assert.NoError(t, err)
		}
		pq.CloseOfBook()
		assert.NoError(t, err)
		wg.Done()
	}()

	// read messages from pbq
	var readMessages []*isb.Message

	go func() {
		for {
			msgs, err := pq.ReadFromPBQ(ctx, 10)
			readMessages = append(readMessages, msgs...)
			if err == EOF {
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()
	// check if we are able to read all the messages
	assert.Len(t, readMessages, len(writeMessages))

	// check if all the messages are persisted in store
	persistedMessages, _ := pq.Store.ReadFromStore(int64(msgsCount))
	assert.Len(t, persistedMessages, len(writeMessages))

	err = pq.GC()
	assert.NoError(t, err)
}

func TestPBQFlowWithStoreFullError(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, store.WithStoreSize(int64(size)), store.WithPbqStoreType("in-memory"), store.WithReadTimeout(1), store.WithBufferSize(10))
	assert.NoError(t, err)

	pq, _, err := pbqManager.GetPBQ(ctx, "partition-5", true, dfv1.InMemoryStoreType)
	assert.NoError(t, err)
	msgsCount := 150
	var wg sync.WaitGroup
	wg.Add(2)

	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())
	var count int

	go func() {
		for _, msg := range writeMessages {
			err := pq.WriteFromISB(&msg)
			if err == store.WriteStoreFullError {
				count += 1
			}
		}
		pq.CloseOfBook()
		assert.NoError(t, err)
		wg.Done()
	}()

	// read messages from pbq
	var readMessages []*isb.Message

	go func() {
		for {
			msgs, err := pq.ReadFromPBQ(ctx, 10)
			readMessages = append(readMessages, msgs...)
			if err == EOF {
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()
	// check if we are able to read all the which are written to pbq
	assert.Len(t, readMessages, msgsCount)
	// since msg count is greater than store size, write to store error count should match msgsCount - size
	assert.Equal(t, count, msgsCount-size)

	persistedMessages, _ := pq.Store.ReadFromStore(int64(msgsCount))
	assert.Len(t, persistedMessages, size)

	err = pq.GC()
	assert.NoError(t, err)
}

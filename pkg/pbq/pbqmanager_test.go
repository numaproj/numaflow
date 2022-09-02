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
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using pbq manager
	var pq1, pq2 *PBQ
	pq1, err = pbqManager.NewPBQ(ctx, "partition-1")
	assert.NoError(t, err)

	pq2, err = pbqManager.NewPBQ(ctx, "partition-2")
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
	var pb1, pb2 *PBQ
	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using Get PBQ
	pb1, err = pbqManager.NewPBQ(ctx, "partition-3")
	assert.NoError(t, err)

	// get the created pbq
	pb2, err = pbqManager.GetPBQ("partition-3")
	assert.NoError(t, err)

	assert.Equal(t, pb1, pb2)
}

// manager -> pbq -> store
func TestPBQFlow(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	var pq *PBQ
	pq, err = pbqManager.NewPBQ(ctx, "partition-4")
	assert.NoError(t, err)
	msgsCount := 5
	var wg sync.WaitGroup
	wg.Add(2)

	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())

	go func() {
		for _, msg := range writeMessages {
			err := pq.Write(ctx, &msg)
			assert.NoError(t, err)
		}
		pq.CloseOfBook()
		assert.NoError(t, err)
		wg.Done()
	}()

	// read messages from pbq
	var readMessages []*isb.Message

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

	wg.Wait()
	// check if we are able to read all the messages
	assert.Len(t, readMessages, len(writeMessages))

	// check if all the messages are persisted in store
	persistedMessages, _, _ := pq.store.Read(int64(msgsCount))
	assert.Len(t, persistedMessages, len(writeMessages))

	err = pq.GC()
	assert.NoError(t, err)
}

func TestPBQFlowWithStoreFullError(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)
	var pq *PBQ
	pq, err = pbqManager.NewPBQ(ctx, "partition-5")
	assert.NoError(t, err)
	msgsCount := 150
	var wg sync.WaitGroup
	wg.Add(2)

	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())
	var count int

	go func() {
		for _, msg := range writeMessages {
			err := pq.Write(ctx, &msg)
			if err == store.WriteStoreFullErr {
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

	wg.Wait()
	// check if we are able to read all the which are written to pbq
	assert.Len(t, readMessages, msgsCount)
	// since msg count is greater than store size, write to store error count should be equal to (msgsCount - size)
	assert.Equal(t, count, msgsCount-size)

	persistedMessages, _, _ := pq.store.Read(int64(msgsCount))
	assert.Len(t, persistedMessages, size)

	err = pq.GC()
	assert.NoError(t, err)
}

func TestManagerWithNoOpStore(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a pbq backed with no op store
	var pq *PBQ
	pq, err = pbqManager.NewPBQ(ctx, "partition-6")
	msgsCount := 50
	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())
	var count int
	var wg sync.WaitGroup
	wg.Add(1)

	// write messages to pbq(with no op store)
	go func() {
		for _, msg := range writeMessages {
			err := pq.Write(ctx, &msg)
			if err == store.WriteStoreFullErr {
				count += 1
			}
		}
		pq.CloseOfBook()
		assert.NoError(t, err)
		wg.Done()
	}()

	var readMessages []*isb.Message
	wg.Add(1)

	// read messages from pbq(with no op store)
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
	wg.Wait()

	assert.Len(t, readMessages, msgsCount)
}

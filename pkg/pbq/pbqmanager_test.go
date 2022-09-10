package pbq

import (
	"context"
	"sync"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
)

//tests for pbqManager (store type - in-memory)

func TestManager_ListPartitions(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a new pbq using pbq manager
	var pq1, pq2 ReadWriteCloser
	pq1, err = pbqManager.CreateNewPBQ(ctx, "partition-1")
	assert.NoError(t, err)

	pq2, err = pbqManager.CreateNewPBQ(ctx, "partition-2")
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
	pb1, err = pbqManager.CreateNewPBQ(ctx, "partition-3")
	assert.NoError(t, err)

	// get the created pbq
	pb2 = pbqManager.GetPBQ("partition-3")

	assert.Equal(t, pb1, pb2)
}

// manager -> pbq -> store
func TestPBQFlow(t *testing.T) {
	size := 100

	ctx := context.Background()
	pbqManager, err := NewManager(ctx, WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		WithReadTimeout(1*time.Second), WithChannelBufferSize(10))
	assert.NoError(t, err)

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, "partition-4")
	assert.NoError(t, err)

	msgsCount := 5
	var wg sync.WaitGroup
	wg.Add(1)

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

	// write messages to pbq
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())
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

	// create a pbq backed with no op store
	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, "partition-6")
	msgsCount := 50
	var wg sync.WaitGroup

	var readMessages []*isb.Message

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
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())
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

	var pq ReadWriteCloser
	pq, err = pbqManager.CreateNewPBQ(ctx, "partition-5")
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)

	var readMessages []*isb.Message
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
	writeMessages := testutils.BuildTestWriteMessages(int64(msgsCount), time.Now())

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

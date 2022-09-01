package pbq

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestPBQ_WriteFromISB(t *testing.T) {

	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-10", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 5
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	var pq *PBQ
	pq, err = NewPBQ(ctx, "partition-10", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	// check if the messages are persisted in store
	storeMessages, _, _ := pq.Store.ReadFromStore(10)
	assert.Len(t, storeMessages, msgCount)
	pq.CloseOfBook()
	// this means we successfully wrote 10 messages to pbq
}

func TestPBQ_ReadFromPBQ(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-12", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	var pq *PBQ
	pq, err = NewPBQ(ctx, "partition-12", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()
	readMessages, _ := pq.ReadFromPBQ(ctx, int64(100))
	// number of messages written should be equal to number of messages read
	assert.Len(t, readMessages, msgCount)
	err = pq.GC()
	assert.NoError(t, err)
}

func TestPBQ_ReadWrite(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-13", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 5
	buffSize := 5
	_ = store.WithBufferSize(int64(buffSize))(options)
	var pq *PBQ
	pq, err = NewPBQ(ctx, "partition-13", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for {
			msgs, err := pq.ReadFromPBQ(context.Background(), 1)
			readMessages = append(readMessages, msgs...)
			if err != nil {
				break
			}
		}
		err := pq.CloseReader()
		assert.NoError(t, err)
		wg.Done()
	}()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}
	pq.CloseOfBook()

	wg.Wait()
	// count of messages read by parallel go routine should be equal to produced messages
	assert.Len(t, readMessages, msgCount)

}

func Test_PBQReadWithCanceledContext(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-14", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	//create a pbq with buffer size 10
	bufferSize := 10
	_ = store.WithBufferSize(int64(bufferSize))(options)
	var pq *PBQ
	pq, err = NewPBQ(ctx, "partition-14", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

	childCtx, cancelFn := context.WithCancel(ctx)

	go func() {
		for {
			msgs, err := pq.ReadFromPBQ(childCtx, 1)
			readMessages = append(readMessages, msgs...)
			if err != nil {
				break
			}
		}
		err := pq.CloseReader()
		assert.NoError(t, err)
		wg.Done()
	}()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)
	//since we are closing the context, it should not block
	cancelFn()
	pq.CloseOfBook()

	wg.Wait()
	assert.Len(t, readMessages, 10)
}

func TestPBQ_WriteWithStoreFull(t *testing.T) {

	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-10", options)
	assert.NoError(t, err)

	// write 101 isb messages to persisted store, but the store size is 100
	msgCount := 101
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 5
	buffSize := 101
	_ = store.WithBufferSize(int64(buffSize))(options)
	var pq *PBQ
	pq, err = NewPBQ(ctx, "partition-10", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err = pq.WriteFromISB(ctx, &msg)
	}
	assert.Error(t, err, store.WriteStoreFullErr)
	// check if the messages are persisted in store
	storeMessages, _, _ := pq.Store.ReadFromStore(100)
	assert.Len(t, storeMessages, storeSize)
	pq.CloseOfBook()
}

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
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
	"github.com/stretchr/testify/assert"
)

func TestPBQ_WriteFromISB(t *testing.T) {

	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryStoreType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryStoreType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "partition-1", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	// check if the messages are persisted in store
	storeMessages, _ := pq.Store.ReadFromStore(10)
	assert.Len(t, storeMessages, msgCount)
	pq.CloseOfBook()
	// this means we successfully wrote 10 messages to pbq
}

func TestPBQ_ReadFromPBQ(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryStoreType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryStoreType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
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
	_ = store.WithPbqStoreType(dfv1.InMemoryStoreType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryStoreType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
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
	_ = store.WithPbqStoreType(dfv1.InMemoryStoreType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryStoreType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	// write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	bufferSize := 10
	_ = store.WithBufferSize(int64(bufferSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

	childCtx, cancelFn := context.WithCancel(ctx)

	go func() {
		//cancelFn()
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

	//since we are closing the context before writing to pbq, read message count should be zero, and it should not block
	cancelFn()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)
	// since we are closing the context, it should not block
	cancelFn()
	pq.CloseOfBook()

	wg.Wait()
	// count of messages read by parallel go routine should be equal to produced messages
	assert.Len(t, readMessages, 0)
}

func TestPBQ_ReadWithEOF(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryStoreType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeoutSecs(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType(dfv1.InMemoryStoreType), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-2", options)
	assert.NoError(t, err)

	// write 50 isb messages to persisted store
	msgCount := 50
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// create a pbq with buffer size 10
	bufferSize := 10
	_ = store.WithBufferSize(int64(bufferSize))(options)
	pq, err := NewPBQ(ctx, "partition-2", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq
	var wg sync.WaitGroup
	wg.Add(1)

	// reading 100 messages from the store, you should get EOF since you only have 50 messages
	go func() {
		var err error
		var msgs []*isb.Message
		for i := 0; i < 100; i++ {
			msgs, err = pq.ReadFromPBQ(ctx, 1)
			readMessages = append(readMessages, msgs...)
		}
		// check for EOF, since there are only 50 messages in store
		assert.Error(t, err, EOF)
		err = pq.CloseReader()
		assert.NoError(t, err)
		wg.Done()
	}()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(ctx, &msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()
	wg.Wait()
	assert.Len(t, readMessages, msgCount)
}

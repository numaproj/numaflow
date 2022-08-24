package pbq

import (
	"context"
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
	_ = store.WithPbqStoreType("in-memory")(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// lets create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "partition-1", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(&msg)
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
	_ = store.WithPbqStoreType("in-memory")(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeout(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// lets create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(&msg)
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
	_ = store.WithPbqStoreType("in-memory")(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeout(1)(options)

	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	//create a pbq with buffer size 10
	buffSize := 10
	_ = store.WithBufferSize(int64(buffSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq and writes to a channel
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
		err := pq.Close()
		assert.NoError(t, err)
		wg.Done()
	}()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(&msg)
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
	_ = store.WithPbqStoreType("in-memory")(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	_ = store.WithReadTimeout(1)(options)
	
	ctx := context.Background()

	qManager, _ := NewManager(ctx, store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	memStore, err := memory.NewMemoryStore(ctx, "partition-1", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	//create a pbq with buffer size 10
	bufferSize := 10
	_ = store.WithBufferSize(int64(bufferSize))(options)
	pq, err := NewPBQ(ctx, "new-partition", memStore, qManager, options)
	assert.NoError(t, err)

	var readMessages []*isb.Message
	// run a parallel go routine which reads from pbq and writes to a channel
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
		//err := pq.Close()
		assert.NoError(t, err)
		wg.Done()
	}()

	//since we are closing the context before writing to pbq, read message count should be zero, and it should not block
	cancelFn()

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(&msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()

	wg.Wait()
	// count of messages read by parallel go routine should be equal to produced messages
	assert.Len(t, readMessages, 0)
}

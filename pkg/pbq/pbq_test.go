package pbq

import (
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
	memStore, err := memory.NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// lets create a pbq with buffer size 10
	buffSize := 10
	pq, err := NewPBQ("newpartition", int64(buffSize), memStore)
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
	memStore, err := memory.NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	// lets create a pbq with buffer size 10
	buffSize := 10
	pq, err := NewPBQ("newpartition", int64(buffSize), memStore)
	assert.NoError(t, err)

	for _, msg := range writeMessages {
		err := pq.WriteFromISB(&msg)
		assert.NoError(t, err)
	}

	pq.CloseOfBook()

	readChannel := pq.ReadFromPBQ()

	var readMessages []*isb.Message

	for msg := range readChannel {
		readMessages = append(readMessages, msg)
	}

	// number of messages written should be equal to number of messages read
	assert.Len(t, readMessages, msgCount)
	err = pq.GC()
	assert.NoError(t, err)
}

func TestPBQ_ReadWrite(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	memStore, err := memory.NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 100
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	//create a pbq with buffer size 10
	buffSize := 2
	pq, err := NewPBQ("newpartition", int64(buffSize), memStore)
	assert.NoError(t, err)

	msgChan := make(chan *isb.Message, msgCount)
	// run a parallel go routine which reads from pbq and writes to a channel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for msg := range pq.ReadFromPBQ() {
			msgChan <- msg
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
	assert.Len(t, msgChan, msgCount)

}

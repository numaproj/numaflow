package memory

import (
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMemoryStore_WriteToStore(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	memStore, err := NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.WriteToStore(&msg)
		assert.NoError(t, err)
	}
}

func TestMemoryStore_ReadFromStore(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	memStore, err := NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.WriteToStore(&msg)
		assert.NoError(t, err)
	}

	readMessages, err := memStore.ReadFromStore(int64(msgCount))
	println(len(readMessages))
	assert.Len(t, readMessages, msgCount)
}

func TestEmptyStore_Read(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	memStore, err := NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	_, err = memStore.ReadFromStore(int64(storeSize))
	assert.ErrorContains(t, err, "no messages")

}

func TestFullStore_Write(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	memStore, err := NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithStoreSize(int64(storeSize)))
	assert.NoError(t, err)

	//write 100 isb messages to persisted store
	msgCount := 100
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.WriteToStore(&msg)
		assert.NoError(t, err)
	}

	// now the store is full, if we write to store we should get an error
	err = memStore.WriteToStore(&writeMessages[0])
	assert.ErrorContains(t, err, "store is full")
}

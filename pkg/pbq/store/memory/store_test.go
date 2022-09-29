package memory

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMemoryStore_WriteToStore(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.StoreOptions{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	partitionID := keyed.PartitionID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "new-partition",
	}

	memStore, err := NewMemoryStore(ctx, partitionID, options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}
}

func TestMemoryStore_ReadFromStore(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.StoreOptions{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	partitionID := keyed.PartitionID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "new-partition",
	}

	memStore, err := NewMemoryStore(ctx, partitionID, options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}
	var readMessages []*isb.Message
	readMessages, _, err = memStore.Read(int64(msgCount))
	assert.NoError(t, err)
	// number of read messages should be equal to msgCount
	assert.Len(t, readMessages, msgCount)
}

func TestEmptyStore_Read(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.StoreOptions{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	ctx := context.Background()

	partitionID := keyed.PartitionID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "new-partition",
	}

	memStore, err := NewMemoryStore(ctx, partitionID, options)
	assert.NoError(t, err)
	var eof bool
	_, eof, err = memStore.Read(int64(storeSize))
	assert.NoError(t, err)
	// since store is empty, eof will be true
	assert.Equal(t, eof, true)

}

func TestFullStore_Write(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.StoreOptions{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	partitionID := keyed.PartitionID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "new-partition",
	}

	memStore, err := NewMemoryStore(ctx, partitionID, options)
	assert.NoError(t, err)

	//write 100 isb messages to persisted store
	msgCount := 100
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}

	// now the store is full, if we write to store we should get an error
	err = memStore.Write(&writeMessages[0])
	assert.ErrorContains(t, err, "store is full")
}

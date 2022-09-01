package memory

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMemoryStore_WriteToStore(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	memStore, err := NewMemoryStore(ctx, "new-partition", options)
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
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	memStore, err := NewMemoryStore(ctx, "new-partition-2", options)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestWriteMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.WriteToStore(&msg)
		assert.NoError(t, err)
	}
	var readMessages []*isb.Message
	readMessages, _, err = memStore.ReadFromStore(int64(msgCount))
	assert.NoError(t, err)
	assert.Len(t, readMessages, msgCount)
}

func TestEmptyStore_Read(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	ctx := context.Background()

	memStore, err := NewMemoryStore(ctx, "new-partition-3", options)
	assert.NoError(t, err)
	var eof bool
	_, eof, err = memStore.ReadFromStore(int64(storeSize))
	assert.Equal(t, eof, true)

}

func TestFullStore_Write(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := 100
	options := &store.Options{}
	_ = store.WithPbqStoreType(dfv1.InMemoryType)(options)
	_ = store.WithStoreSize(int64(storeSize))(options)
	ctx := context.Background()

	memStore, err := NewMemoryStore(ctx, "new-partition-4", options)
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

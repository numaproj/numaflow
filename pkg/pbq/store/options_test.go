package store

import (
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	testOpts := []StoreOption{
		WithStoreSize(100),
		WithMaxBufferSize(10),
		WithPbqStoreType(v1alpha1.InMemoryType),
		WithSyncDuration(2 * time.Second),
	}

	storeOptions := &StoreOptions{
		maxBatchSize: 0,
		syncDuration: 1 * time.Second,
		pbqStoreType: "in-memory",
		storeSize:    0,
	}

	for _, opt := range testOpts {
		err := opt(storeOptions)
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(100), storeOptions.storeSize)
	assert.Equal(t, int64(10), storeOptions.maxBatchSize)
	assert.Equal(t, 2*time.Second, storeOptions.syncDuration)
	assert.Equal(t, v1alpha1.InMemoryType, storeOptions.pbqStoreType)
}

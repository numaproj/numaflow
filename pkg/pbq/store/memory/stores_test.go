package memory

import (
	"context"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/stretchr/testify/assert"
)

func TestMemoryStores(t *testing.T) {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	partitionIds := []partition.ID{
		{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Key:   "test-1",
		},
		{
			Start: time.Unix(120, 0),
			End:   time.Unix(180, 0),
			Key:   "test-2",
		},
		{
			Start: time.Unix(180, 0),
			End:   time.Unix(240, 0),
			Key:   "test-3",
		},
	}
	storeProvider := NewMemoryStores(WithStoreSize(100))

	for _, partitionID := range partitionIds {
		_, err := storeProvider.CreateStore(ctx, partitionID)
		assert.NoError(t, err)
	}

	var discoveredPartitions []partition.ID
	discoveredPartitions, err = storeProvider.DiscoverPartitions(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoveredPartitions, len(partitionIds))

	for _, partitionID := range partitionIds {
		err = storeProvider.DeleteStore(partitionID)
		assert.NoError(t, err)
	}

	discoveredPartitions, err = storeProvider.DiscoverPartitions(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoveredPartitions, 0)
}

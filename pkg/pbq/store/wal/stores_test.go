package wal

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/stretchr/testify/assert"
)

func TestWalStores(t *testing.T) {
	vi = &dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
			},
		}},
		Hostname: "test-host",
		Replica:  0,
	}
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

	tmp := t.TempDir()
	storeProvider := NewWALStores(vi, WithStorePath(tmp))

	for _, partitionID := range partitionIds {
		_, err = storeProvider.CreateStore(ctx, partitionID)
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

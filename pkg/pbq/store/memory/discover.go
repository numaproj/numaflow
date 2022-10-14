package memory

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

var discoverer func(*store.StoreOptions) ([]partition.ID, error)

// DiscoverPartitions discovers the in-memory partitions. This is very useful during testing.
func DiscoverPartitions(ctx context.Context, options *store.StoreOptions) ([]partition.ID, error) {
	if discoverer == nil {
		return []partition.ID{}, nil
	}
	return discoverer(options)
}

// SetDiscoverer sets the function that returns the initial list of partitions
// Only for testing purposes
func SetDiscoverer(d func(*store.StoreOptions) ([]partition.ID, error)) {
	discoverer = d
}

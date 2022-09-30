package memory

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the in-memory partitions. This is very useful during testing.
// TODO(Yashash): please integrate this with testing.
func DiscoverPartitions(ctx context.Context, options ...*store.StoreOptions) ([]partition.ID, error) {
	return []partition.ID{}, nil
}

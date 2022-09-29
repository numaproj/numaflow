package memory

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the inmemory partitions. This is very useful during testing.
// TODO(Yashash): please integrate this with testing.
func DiscoverPartitions(ctx context.Context, options ...*store.StoreOptions) ([]pbq.ID, error) {
	return []pbq.ID{}, nil
}

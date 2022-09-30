package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

func DiscoverPartitions(ctx context.Context, options ...*store.StoreOptions) ([]partition.ID, error) {
	return []partition.ID{}, nil
}

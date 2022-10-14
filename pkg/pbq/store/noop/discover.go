package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

func DiscoverPartitions(_ context.Context, _ *store.StoreOptions) ([]partition.ID, error) {
	return []partition.ID{}, nil
}

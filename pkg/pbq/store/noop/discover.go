package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

func DiscoverPartitions(ctx context.Context, options ...*store.StoreOptions) ([]pbq.ID, error) {
	return []pbq.ID{}, nil
}

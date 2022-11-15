package wal

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the file system partitions. This is very useful during testing.
func DiscoverPartitions(ctx context.Context, options *store.StoreOptions) ([]partition.ID, error) {
	dir := options.StorePath()
	files, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return []partition.ID{}, nil
	} else if err != nil {
		return nil, err
	}
	partitions := make([]partition.ID, 0)

	for _, f := range files {
		if strings.HasPrefix(f.Name(), SegmentPrefix) && !f.IsDir() {
			filePath := filepath.Join(dir, f.Name())
			wal, err := OpenWAL(filePath)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, *wal.partitionID)
		}
	}

	return partitions, nil
}

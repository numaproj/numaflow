package wal

import (
	"context"
	"os"
	"path/filepath"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the file system partitions. This is very useful during testing.
func DiscoverPartitions(ctx context.Context, options *store.StoreOptions) ([]partition.ID, error) {
	dir := options.StorePath()
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	partitions := make([]partition.ID, 0)

	for _, f := range files {
		filePath := filepath.Join(dir, f.Name())
		stat, err := os.Stat(filePath)
		if err != nil {
			return nil, err
		}
		fp, err := os.OpenFile(filePath, os.O_RDWR, stat.Mode())
		if err != nil {
			return nil, err
		}
		wal := &WAL{
			fp:       fp,
			openMode: os.O_RDWR,
			wOffset:  0,
			rOffset:  0,
			readUpTo: stat.Size(),
		}
		readPartition, err := wal.readHeader()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, *readPartition)
	}

	return partitions, nil
}

/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wal

import (
	"context"
	"os"
	"path/filepath"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the file system partitions. This is very useful during testing.
func DiscoverPartitions(_ context.Context, options *store.StoreOptions) ([]partition.ID, error) {
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

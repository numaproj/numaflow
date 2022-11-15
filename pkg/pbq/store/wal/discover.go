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
	"strings"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

// DiscoverPartitions discovers the file system partitions. This is very useful during testing.
func DiscoverPartitions(_ context.Context, options *store.StoreOptions) ([]partition.ID, error) {
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

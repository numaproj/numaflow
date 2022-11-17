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

package memory

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type memoryStores struct {
	storeSize    int64
	discoverFunc func(ctx context.Context) ([]partition.ID, error)
}

func NewMemoryStores(opts ...Option) store.StoreProvider {
	s := &memoryStores{
		storeSize: 100,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (ms *memoryStores) CreatStore(ctx context.Context, partitionID partition.ID) (store.Store, error) {
	memStore := &memoryStore{
		writePos:    0,
		readPos:     0,
		closed:      false,
		storage:     make([]*isb.ReadMessage, ms.storeSize),
		storeSize:   ms.storeSize,
		log:         logging.FromContext(ctx).With("pbqStore", "Memory").With("partitionID", partitionID),
		partitionID: partitionID,
	}

	return memStore, nil
}

func (ms *memoryStores) DiscoverPartitions(ctx context.Context) ([]partition.ID, error) {
	if ms.discoverFunc != nil {
		return ms.discoverFunc(ctx)
	}
	return []partition.ID{}, nil
}

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
	"errors"
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type memManager struct {
	storeSize    int64
	discoverFunc func(ctx context.Context) ([]wal.WAL, error)
	partitions   map[partition.ID]*memoryStore
	sync.RWMutex
}

func NewMemManager(opts ...Option) wal.Manager {
	s := &memManager{
		storeSize:  100000,
		partitions: make(map[partition.ID]*memoryStore),
	}

	for _, o := range opts {
		o(s)
	}
	return s
}

func (ms *memManager) CreateWAL(ctx context.Context, partitionID partition.ID) (wal.WAL, error) {
	ms.Lock()
	defer ms.Unlock()
	if memStore, ok := ms.partitions[partitionID]; ok {
		return memStore, nil
	}
	memStore := &memoryStore{
		writePos:    0,
		readPos:     0,
		closed:      false,
		storage:     make([]*isb.ReadMessage, ms.storeSize),
		storeSize:   ms.storeSize,
		log:         logging.FromContext(ctx).With("pbqStore", "Memory").With("partitionID", partitionID),
		partitionID: partitionID,
	}
	ms.partitions[partitionID] = memStore
	return memStore, nil
}

func (ms *memManager) DiscoverWALs(ctx context.Context) ([]wal.WAL, error) {
	ms.RLock()
	defer ms.RUnlock()
	if ms.discoverFunc == nil {
		s := make([]wal.WAL, 0)
		for _, val := range ms.partitions {
			s = append(s, val)
		}
		return s, nil
	}
	return ms.discoverFunc(ctx)
}

func (ms *memManager) DeleteWAL(partitionID partition.ID) error {
	ms.Lock()
	defer ms.Unlock()
	memStore, ok := ms.partitions[partitionID]
	if !ok {
		return errors.New("store not found")
	}

	memStore.storage = nil
	memStore.writePos = -1
	delete(ms.partitions, partitionID)
	return nil
}

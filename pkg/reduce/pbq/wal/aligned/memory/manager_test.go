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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

func TestMemoryStores(t *testing.T) {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	partitionIds := []partition.ID{
		{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test-1",
		},
		{
			Start: time.Unix(120, 0),
			End:   time.Unix(180, 0),
			Slot:  "test-2",
		},
		{
			Start: time.Unix(180, 0),
			End:   time.Unix(240, 0),
			Slot:  "test-3",
		},
	}
	storeProvider := NewMemManager(WithStoreSize(100))

	for _, partitionID := range partitionIds {
		_, err := storeProvider.CreateWAL(ctx, partitionID)
		assert.NoError(t, err)
	}

	var discoveredStores []wal.WAL
	discoveredStores, err = storeProvider.DiscoverWALs(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoveredStores, len(partitionIds))

	for _, partitionID := range partitionIds {
		err = storeProvider.DeleteWAL(partitionID)
		assert.NoError(t, err)
	}

	discoveredStores, err = storeProvider.DiscoverWALs(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoveredStores, 0)
}

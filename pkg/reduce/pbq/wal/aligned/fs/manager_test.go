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

package fs

import (
	"context"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"

	"github.com/stretchr/testify/assert"
)

func TestWalStores(t *testing.T) {
	vi = &dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
			},
		}},
		Hostname: "test-host",
		Replica:  0,
	}
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

	tmp := t.TempDir()
	storeProvider := NewFSManager(vi, WithStorePath(tmp))

	for _, partitionID := range partitionIds {
		_, err = storeProvider.CreateWAL(ctx, partitionID)
		assert.NoError(t, err)
	}

	discoverStores, err := storeProvider.DiscoverWALs(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoverStores, len(partitionIds))

	for _, partitionID := range partitionIds {
		err = storeProvider.DeleteWAL(partitionID)
		assert.NoError(t, err)
	}

	discoverStores, err = storeProvider.DiscoverWALs(ctx)
	assert.NoError(t, err)

	assert.Len(t, discoverStores, 0)
}

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

package store

import (
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	testOpts := []StoreOption{
		WithStoreSize(100),
		WithMaxBufferSize(10),
		WithPbqStoreType(v1alpha1.InMemoryType),
		WithSyncDuration(2 * time.Second),
	}

	storeOptions := &StoreOptions{
		maxBatchSize: 0,
		syncDuration: 1 * time.Second,
		pbqStoreType: "in-memory",
		storeSize:    0,
	}

	for _, opt := range testOpts {
		err := opt(storeOptions)
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(100), storeOptions.storeSize)
	assert.Equal(t, int64(10), storeOptions.maxBatchSize)
	assert.Equal(t, 2*time.Second, storeOptions.syncDuration)
	assert.Equal(t, v1alpha1.InMemoryType, storeOptions.pbqStoreType)
}

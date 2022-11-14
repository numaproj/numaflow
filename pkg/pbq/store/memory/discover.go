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

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

var discoverer func(*store.StoreOptions) ([]partition.ID, error)

// DiscoverPartitions discovers the in-memory partitions. This is very useful during testing.
func DiscoverPartitions(ctx context.Context, options *store.StoreOptions) ([]partition.ID, error) {
	if discoverer == nil {
		return []partition.ID{}, nil
	}
	return discoverer(options)
}

// SetDiscoverer sets the function that returns the initial list of partitions
// Only for testing purposes
func SetDiscoverer(d func(*store.StoreOptions) ([]partition.ID, error)) {
	discoverer = d
}

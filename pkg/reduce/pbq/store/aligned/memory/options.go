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

	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
)

type Option func(stores *memoryStores)

// WithDiscoverer sets the discover func of memorystores
func WithDiscoverer(f func(ctx context.Context) ([]store.Store, error)) Option {
	return func(stores *memoryStores) {
		stores.discoverFunc = f
	}
}

// WithStoreSize sets the store size
func WithStoreSize(size int64) Option {
	return func(stores *memoryStores) {
		stores.storeSize = size
	}
}

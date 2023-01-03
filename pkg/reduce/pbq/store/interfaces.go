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
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// Store provides methods to read, write and delete data from the store.
type Store interface {
	// Read returns upto N(size) messages from the persisted store, it also returns
	// a boolean flag to indicate if the end of file has been reached.
	Read(size int64) ([]*isb.ReadMessage, bool, error)
	// Write writes message to persistence store
	Write(msg *isb.ReadMessage) error
	// Close closes store
	Close() error
}

// StoreProvider defines the functions for store implementation
type StoreProvider interface {
	// CreateStore returns a new store instance.
	CreateStore(context.Context, partition.ID) (Store, error)
	// DiscoverPartitions discovers all the managed partitions.
	DiscoverPartitions(context.Context) ([]partition.ID, error)
	// DeleteStore deletes the store
	DeleteStore(partition.ID) error
}

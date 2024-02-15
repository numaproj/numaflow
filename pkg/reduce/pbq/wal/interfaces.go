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

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// WAL provides methods to read, write and delete data from the persistence store.
type WAL interface {
	// Replay to replay persisted messages during startup
	// returns a channel to read messages and a channel to read errors
	Replay() (<-chan *isb.ReadMessage, <-chan error)
	// Write writes message to persistence store
	Write(msg *isb.ReadMessage) error
	// PartitionID returns the partition ID of the store
	PartitionID() partition.ID
	// Close closes store
	Close() error
}

// Manager defines the interface to manage the stores.
type Manager interface {
	// CreateWAL returns a new wal instance.
	CreateWAL(context.Context, partition.ID) (WAL, error)
	// DiscoverWALs discovers all the existing wals.
	// This is used to recover from a crash and replay all the messages from the store.
	DiscoverWALs(context.Context) ([]WAL, error)
	// DeleteWAL deletes the wal.
	DeleteWAL(partition.ID) error
}

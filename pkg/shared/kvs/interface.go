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

package kvs

import (
	"context"
)

// KVStorer defines the storage for publishing the watermark and sideinput
type KVStorer interface {
	// GetAllKeys the keys from KV store.
	GetAllKeys(context.Context) ([]string, error)
	// DeleteKey deletes the key from KV store.
	DeleteKey(context.Context, string) error
	// PutKV inserts a key-value pair into the KV store.
	PutKV(context.Context, string, []byte) error
	// GetValue gets the value of the given key.
	GetValue(context.Context, string) ([]byte, error)
	// GetStoreName returns the bucket name of the KV store.
	GetStoreName() string
	// Watch starts watching the KV store for changes. It returns a channel of KVEntry.
	// The KVEntry channel is used to read the KVEntry and will be closed when the store is closed.
	Watch(context.Context) <-chan KVEntry
	// Close closes the backend connection
	Close()
}

// KVWatchOp is the operation as detected by the KV watcher.
type KVWatchOp int64

const (
	// KVPut indicates an element has been put/added into the KV store.
	KVPut KVWatchOp = iota
	// KVDelete represents a delete.
	KVDelete
	// KVPurge is when the kv bucket is purged.
	// This value is only for JetStream.
	KVPurge
)

func (kvOp KVWatchOp) String() string {
	switch kvOp {
	case KVPut:
		return "KVPut"
	case KVDelete:
		return "KVDelete"
	case KVPurge:
		return "KVPurge"
	default:
		return "UnknownOP"
	}
}

// KVEntry defines what can be read on the Watch stream.
type KVEntry interface {
	// Key is the key that was retrieved.
	Key() string
	// Value is the retrieved value.
	Value() []byte
	// Operation returns `KVWatchOp`.
	Operation() KVWatchOp
}

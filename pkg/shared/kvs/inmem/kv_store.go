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

/*
Package inmem package implements the watermark progression using in mem store as the KV store.
*/
package inmem

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// kvEntry is each key-value entry in the store and the operation associated with the kv pair.
type kvEntry struct {
	key   string
	value []byte
	op    kvs.KVWatchOp
}

// Key returns the key
func (k kvEntry) Key() string {
	return k.key
}

// Value returns the value.
func (k kvEntry) Value() []byte {
	return k.value
}

// Operation returns the operation on that key-value pair.
func (k kvEntry) Operation() kvs.KVWatchOp {
	return k.op
}

// inMemWatcher is a watcher for the in mem key-value store.
type inMemWatcher struct {
	updates chan kvs.KVEntry
	stopped chan struct{}
}

// inMemStore implements the watermark's KV store backed up by in mem store.
type inMemStore struct {
	bucketName string
	kv         map[string][]byte
	lock       sync.RWMutex
	// kvHistory is an ever an growing list of histories
	kvHistory []kvs.KVEntry
	// watchers is an active list of watchers.
	watchers []*inMemWatcher
	isClosed bool
	log      *zap.SugaredLogger
}

var _ kvs.KVStorer = (*inMemStore)(nil)

// NewKVInMemKVStore returns inMemStore.
func NewKVInMemKVStore(ctx context.Context, bucketName string) (kvs.KVStorer, error) {
	s := &inMemStore{
		bucketName: bucketName,
		kv:         make(map[string][]byte),
		kvHistory:  make([]kvs.KVEntry, 0),
		watchers:   make([]*inMemWatcher, 0),
		log:        logging.FromContext(ctx).With("bucketName", bucketName),
	}

	return s, nil
}

// GetAllKeys returns all the keys in the key-value store.
func (kv *inMemStore) GetAllKeys(_ context.Context) ([]string, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	var keys []string
	for key := range kv.kv {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i int, j int) bool {
		return keys[i] < keys[j]
	})
	return keys, nil
}

// GetValue returns the value for a given key.
func (kv *inMemStore) GetValue(_ context.Context, k string) ([]byte, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	if val, ok := kv.kv[k]; ok {
		return val, nil
	} else {
		return []byte(""), fmt.Errorf("key %s not found", k)
	}
}

// GetStoreName returns the store name.
func (kv *inMemStore) GetStoreName() string {
	return kv.bucketName
}

// DeleteKey deletes the key from the in mem key-value store.
func (kv *inMemStore) DeleteKey(ctx context.Context, k string) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if val, ok := kv.kv[k]; ok {
		delete(kv.kv, k)
		entry := kvEntry{
			key:   k,
			value: val,
			op:    kvs.KVDelete,
		}

		// notify all the watchers about the delete operation
		for _, watcher := range kv.watchers {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case watcher.updates <- entry:
			}
		}
		kv.kvHistory = append(kv.kvHistory, entry)
		return nil
	} else {
		return fmt.Errorf("key %s not found", k)
	}
}

// PutKV puts an element to the in mem key-value store.
func (kv *inMemStore) PutKV(ctx context.Context, k string, v []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	if kv.isClosed {
		return fmt.Errorf("kv store is closed")
	}
	var val = make([]byte, len(v))
	copy(val, v)
	kv.kv[k] = val
	entry := kvEntry{
		key:   k,
		value: val,
		op:    kvs.KVPut,
	}

	// notify all the watchers about the put operation
	for _, watcher := range kv.watchers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watcher.updates <- entry:
		}
	}
	kv.kvHistory = append(kv.kvHistory, entry)
	return nil
}

// Watch watches the key-value store and returns the "updates channel" and a done channel.
func (kv *inMemStore) Watch(ctx context.Context) (<-chan kvs.KVEntry, <-chan struct{}) {
	// create a new updates channel and fill in the history
	var updates = make(chan kvs.KVEntry)
	var stopped = make(chan struct{})

	// for new updates channel initialization
	go func() {
		kv.lock.Lock()
		// fill in the history for the new updates channel
		for _, value := range kv.kvHistory {
			select {
			case <-ctx.Done():
				return
			case updates <- value:
			}
		}
		// add it to the list of watchers
		kv.watchers = append(kv.watchers, &inMemWatcher{
			updates: updates,
			stopped: stopped,
		})
		kv.lock.Unlock()
	}()
	return updates, stopped
}

// Close closes the in mem key-value store. It will close all the watchers.
func (kv *inMemStore) Close() {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.isClosed = true
	for _, watcher := range kv.watchers {
		close(watcher.updates)
		close(watcher.stopped)
	}
}

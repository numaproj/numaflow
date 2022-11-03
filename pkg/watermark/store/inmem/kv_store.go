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

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

var (
	buckets     = make(map[string]*inMemStore)
	bucketsLock sync.RWMutex
)

// kvEntry is each key-value entry in the store and the operation associated with the kv pair.
type kvEntry struct {
	key   string
	value []byte
	op    store.KVWatchOp
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
func (k kvEntry) Operation() store.KVWatchOp {
	return k.op
}

// inMemStore implements the watermark's KV store backed up by in mem store.
type inMemStore struct {
	pipelineName string
	bucketName   string
	kv           map[string][]byte
	kvLock       sync.RWMutex
	kvEntryCh    chan store.WatermarkKVEntry
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*inMemStore)(nil)

// NewKVInMemKVStore returns inMemStore.
func NewKVInMemKVStore(ctx context.Context, pipelineName string, bucketName string) (store.WatermarkKVStorer, chan store.WatermarkKVEntry, error) {
	s := &inMemStore{
		pipelineName: pipelineName,
		bucketName:   bucketName,
		kv:           make(map[string][]byte),
		kvEntryCh:    make(chan store.WatermarkKVEntry, 10),
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}
	bucketsLock.Lock()
	buckets[bucketName] = s
	bucketsLock.Unlock()
	return s, s.kvEntryCh, nil
}

// GetAllKeys returns all the keys in the key-value store.
func (kv *inMemStore) GetAllKeys(_ context.Context) ([]string, error) {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
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
	kv.kvLock.RLock()
	defer kv.kvLock.RUnlock()
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
func (kv *inMemStore) DeleteKey(_ context.Context, k string) error {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	if val, ok := kv.kv[k]; ok {
		delete(kv.kv, k)
		kv.kvEntryCh <- kvEntry{
			key:   k,
			value: val,
			op:    store.KVDelete,
		}
		return nil
	} else {
		return fmt.Errorf("key %s not found", k)
	}
}

// PutKV puts an element to the in mem key-value store.
func (kv *inMemStore) PutKV(_ context.Context, k string, v []byte) error {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	var val = make([]byte, len(v))
	copy(val, v)
	kv.kv[k] = val
	kv.kvEntryCh <- kvEntry{
		key:   k,
		value: val,
		op:    store.KVPut,
	}
	return nil
}

// Close closes the channel connection and clean up the bucket.
func (kv *inMemStore) Close() {
	close(kv.kvEntryCh)
	bucketsLock.Lock()
	delete(buckets, kv.bucketName)
	bucketsLock.Unlock()
}

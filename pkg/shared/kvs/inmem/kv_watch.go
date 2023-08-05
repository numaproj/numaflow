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

package inmem

import (
	"context"
	"sync"

	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// inMemWatch implements the watermark's KV store backed up by in memory store.
type inMemWatch struct {
	pipelineName string
	bucketName   string
	kvEntryCh    <-chan kvs.KVEntry
	kvHistory    []kvs.KVEntry
	updatesChMap map[string]chan kvs.KVEntry
	lock         sync.Mutex // the lock for kvHistory and updatesChMap
	log          *zap.SugaredLogger
}

var _ kvs.KVWatcher = (*inMemWatch)(nil)

// NewInMemWatch returns inMemWatch which implements the KVWatcher interface.
func NewInMemWatch(ctx context.Context, pipelineName string, bucketName string, kvEntryCh <-chan kvs.KVEntry) (kvs.KVWatcher, error) {
	k := &inMemWatch{
		pipelineName: pipelineName,
		bucketName:   bucketName,
		kvEntryCh:    kvEntryCh,
		kvHistory:    []kvs.KVEntry{},
		updatesChMap: make(map[string]chan kvs.KVEntry),
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}
	return k, nil
}

// Watch watches the key-value store.
func (k *inMemWatch) Watch(ctx context.Context) (<-chan kvs.KVEntry, <-chan struct{}) {
	// create a new updates channel and fill in the history
	var id = util.RandomString(10)
	var updates = make(chan kvs.KVEntry)
	var stopped = make(chan struct{})

	// for new updates channel initialization
	go func() {
		k.lock.Lock()
		for _, value := range k.kvHistory {
			updates <- value
		}
		// add the new updates channel to the map before unlock
		// so the new updates channel won't miss new kv operations
		k.updatesChMap[id] = updates
		k.lock.Unlock()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				k.log.Infow("stopping watching", zap.String("watcher", k.GetKVName()))
				k.lock.Lock()
				delete(k.updatesChMap, id)
				close(updates)
				k.lock.Unlock()
				close(stopped)
				return
			case value := <-k.kvEntryCh:
				k.log.Debug(value.Key(), value.Value(), value.Operation())
				k.lock.Lock()
				k.kvHistory = append(k.kvHistory, value)
				for _, ch := range k.updatesChMap {
					ch <- value
				}
				k.lock.Unlock()
			}
		}
	}()
	return updates, stopped
}

// GetKVName returns the KV store (bucket) name.
func (k *inMemWatch) GetKVName() string {
	return k.bucketName
}

// Close does nothing here because the updates channel has already been closed when exiting the Watch.
func (k *inMemWatch) Close() {
}

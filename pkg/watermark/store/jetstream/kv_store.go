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
Package jetstream package implements the watermark progression using Jetstream as the KV store.
*/
package jetstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// jetStreamStore implements the watermark's KV store backed up by Jetstream.
type jetStreamStore struct {
	pipelineName string
	conn         *jsclient.NatsConn
	kv           nats.KeyValue
	kvLock       sync.Mutex
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*jetStreamStore)(nil)

// NewKVJetStreamKVStore returns KVJetStreamStore.
func NewKVJetStreamKVStore(ctx context.Context, pipelineName string, bucketName string, client jsclient.JetStreamClient, opts ...JSKVStoreOption) (store.WatermarkKVStorer, error) {
	var err error
	var j *jetStreamStore
	conn, err := client.Connect(ctx, jsclient.ReconnectHandler(func(_ *jsclient.NatsConn) {
		if j != nil && j.js != nil {
			// create a new KV
			kv, err := j.js.KeyValue(bucketName)
			// keep looping because the watermark won't work without a watcher
			for err != nil {
				j.log.Errorw("Failed to create JetStream KeyValue", zap.Error(err))
				kv, err = j.js.KeyValue(bucketName)
				time.Sleep(100 * time.Millisecond)
			}
			j.log.Infow("Succeeded to recreate JetStream KeyValue store")
			j.kv = kv
		}
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	// do we need to specify any opts? if yes, send it via options.
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j = &jetStreamStore{
		pipelineName: pipelineName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	// for JetStream KeyValue store, the bucket should have been created in advance
	j.kv, err = j.js.KeyValue(bucketName)
	if err != nil {
		j.Close()
		return nil, err
	}
	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			j.Close()
			return nil, err
		}
	}
	return j, nil
}

// JSKVStoreOption is to pass in Jetstream options.
type JSKVStoreOption func(*jetStreamStore) error

// GetAllKeys returns all the keys in the key-value store.
func (kv *jetStreamStore) GetAllKeys(_ context.Context) ([]string, error) {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	keys, err := kv.kv.Keys()
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (kv *jetStreamStore) GetValue(_ context.Context, k string) ([]byte, error) {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	kvEntry, err := kv.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := kvEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (kv *jetStreamStore) GetStoreName() string {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	return kv.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (kv *jetStreamStore) DeleteKey(_ context.Context, k string) error {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	// will return error if nats connection is closed
	return kv.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (kv *jetStreamStore) PutKV(_ context.Context, k string, v []byte) error {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	// will return error if nats connection is closed
	_, err := kv.kv.Put(k, v)
	return err
}

// Close closes the jetstream connection.
func (kv *jetStreamStore) Close() {
	kv.kvLock.Lock()
	defer kv.kvLock.Unlock()
	if !kv.conn.IsClosed() {
		kv.conn.Close()
	}
}

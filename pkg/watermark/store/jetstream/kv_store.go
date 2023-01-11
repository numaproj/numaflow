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
	kvLock       sync.RWMutex
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*jetStreamStore)(nil)

// NewKVJetStreamKVStore returns KVJetStreamStore.
func NewKVJetStreamKVStore(ctx context.Context, pipelineName string, bucketName string, client jsclient.JetStreamClient, opts ...JSKVStoreOption) (store.WatermarkKVStorer, error) {
	var err error
	var jsStore *jetStreamStore
	conn, err := client.Connect(ctx, jsclient.ReconnectHandler(func(_ *jsclient.NatsConn) {
		if jsStore != nil && jsStore.js != nil {
			// re-bind to an existing KeyValue store
			kv, err := jsStore.js.KeyValue(bucketName)
			// keep looping because the watermark won't work without the store
			for err != nil {
				jsStore.log.Errorw("Failed to rebind to the JetStream KeyValue store ", zap.Error(err))
				kv, err = jsStore.js.KeyValue(bucketName)
				time.Sleep(100 * time.Millisecond)
			}
			jsStore.log.Infow("Succeeded to rebind to JetStream KeyValue store")
			jsStore.kvLock.Lock()
			defer jsStore.kvLock.Unlock()
			jsStore.kv = kv
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

	jsStore = &jetStreamStore{
		pipelineName: pipelineName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	// for JetStream KeyValue store, the bucket should have been created in advance
	jsStore.kv, err = jsStore.js.KeyValue(bucketName)
	if err != nil {
		jsStore.Close()
		return nil, err
	}
	// options if any
	for _, o := range opts {
		if err := o(jsStore); err != nil {
			jsStore.Close()
			return nil, err
		}
	}
	return jsStore, nil
}

// JSKVStoreOption is to pass in JetStream options.
type JSKVStoreOption func(*jetStreamStore) error

// GetAllKeys returns all the keys in the key-value store.
func (jss *jetStreamStore) GetAllKeys(_ context.Context) ([]string, error) {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	keys, err := jss.kv.Keys()
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (jss *jetStreamStore) GetValue(_ context.Context, k string) ([]byte, error) {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	kvEntry, err := jss.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := kvEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (jss *jetStreamStore) GetStoreName() string {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	return jss.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (jss *jetStreamStore) DeleteKey(_ context.Context, k string) error {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	// will return error if nats connection is closed
	return jss.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (jss *jetStreamStore) PutKV(_ context.Context, k string, v []byte) error {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	// will return error if nats connection is closed
	_, err := jss.kv.Put(k, v)
	return err
}

// Close closes the JetStream connection.
func (jss *jetStreamStore) Close() {
	jss.kvLock.RLock()
	defer jss.kvLock.RUnlock()
	if !jss.conn.IsClosed() {
		jss.conn.Close()
	}
}

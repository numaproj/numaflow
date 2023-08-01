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
	"sync"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// jetStreamStore implements the watermark's KV store backed up by Jetstream.
type jetStreamStore struct {
	pipelineName string
	client       *jsclient.NATSClient
	kv           nats.KeyValue
	kvLock       sync.RWMutex
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*jetStreamStore)(nil)

// NewKVJetStreamKVStore returns KVJetStreamStore.
func NewKVJetStreamKVStore(ctx context.Context, pipelineName string, bucketName string, client *jsclient.NATSClient, opts ...JSKVStoreOption) (store.WatermarkKVStorer, error) {
	var err error
	var jsStore = &jetStreamStore{
		pipelineName: pipelineName,
		client:       client,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	// for JetStream KeyValue store, the bucket should have been created in advance
	jsStore.kv, err = jsStore.client.BindKVStore(bucketName)
	if err != nil {
		return nil, err
	}
	// options if any
	for _, o := range opts {
		if err := o(jsStore); err != nil {
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

// Close we don't need to close the JetStream connection. It will be closed by the caller.
func (jss *jetStreamStore) Close() {
}

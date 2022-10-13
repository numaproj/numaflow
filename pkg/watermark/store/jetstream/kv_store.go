/*
Package jetstream package implements the watermark progression using Jetstream as the KV store.
*/
package jetstream

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// jetStreamStore implements the watermark's KV store backed up by Jetstream.
type jetStreamStore struct {
	pipelineName string
	conn         *jsclient.NatsConn
	kv           nats.KeyValue
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*jetStreamStore)(nil)

// NewKVJetStreamKVStore returns KVJetStreamStore.
func NewKVJetStreamKVStore(ctx context.Context, pipelineName string, bucketName string, client jsclient.JetStreamClient, opts ...JSKVStoreOption) (store.WatermarkKVStorer, error) {
	var err error
	conn, err := client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	// do we need to specify any opts? if yes, send it via options.
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j := &jetStreamStore{
		pipelineName: pipelineName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	// for JetStream KeyValue store, the bucket should have been created in advance
	j.kv, err = j.js.KeyValue(bucketName)
	if err != nil {
		return nil, err
	}
	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// JSKVStoreOption is to pass in Jetstream options.
type JSKVStoreOption func(*jetStreamStore) error

// GetAllKeys returns all the keys in the key-value store.
func (kv *jetStreamStore) GetAllKeys(_ context.Context) ([]string, error) {
	keys, err := kv.kv.Keys()
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (kv *jetStreamStore) GetValue(_ context.Context, k string) ([]byte, error) {
	kvEntry, err := kv.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := kvEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (kv *jetStreamStore) GetStoreName() string {
	return kv.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (kv *jetStreamStore) DeleteKey(_ context.Context, k string) error {
	return kv.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (kv *jetStreamStore) PutKV(_ context.Context, k string, v []byte) error {
	_, err := kv.kv.Put(k, v)
	return err
}

// Close closes the jetstream connection.
func (kv *jetStreamStore) Close() {
	if !kv.conn.IsClosed() {
		kv.conn.Close()
	}
}

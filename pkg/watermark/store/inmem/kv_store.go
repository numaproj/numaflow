/*
Package inmem package implements the watermark progression using in mem store as the KV store.
*/
package inmem

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// inMemStore implements the watermark's KV store backed up by Jetstream.
type inMemStore struct {
	pipelineName string
	conn         *jsclient.NatsConn
	kv           nats.KeyValue
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVStorer = (*inMemStore)(nil)

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

	j := &inMemStore{
		pipelineName: pipelineName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

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
type JSKVStoreOption func(*inMemStore) error

// GetAllKeys returns all the keys in the key-value store.
func (kv *inMemStore) GetAllKeys(_ context.Context) ([]string, error) {
	keys, err := kv.kv.Keys()
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (kv *inMemStore) GetValue(_ context.Context, k string) ([]byte, error) {
	kvEntry, err := kv.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := kvEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (kv *inMemStore) GetStoreName() string {
	return kv.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (kv *inMemStore) DeleteKey(_ context.Context, k string) error {
	return kv.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (kv *inMemStore) PutKV(_ context.Context, k string, v []byte) error {
	_, err := kv.kv.Put(k, v)
	return err
}

// Close closes the jetstream connection.
func (kv *inMemStore) Close() {
	if !kv.conn.IsClosed() {
		kv.conn.Close()
	}
}

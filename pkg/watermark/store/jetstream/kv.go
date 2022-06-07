/*
Package jetstream package implements the watermark progression using Jetstream as the KV store.
*/
package jetstream

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// KVJetStreamSvc implements the watermark's KV store backed up by Jetstream.
type KVJetStreamSvc struct {
	pipelineName string
	kv           nats.KeyValue
	js           nats.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.PublisherKVStorer = (*KVJetStreamSvc)(nil)

func NewKVJetStreamSvc(ctx context.Context, bucketName string, pipelineName string, js nats.JetStreamContext, opts ...JSServiceOption) (*KVJetStreamSvc, error) {
	var err error
	j := &KVJetStreamSvc{
		pipelineName: pipelineName,
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

// JSServiceOption is to pass in Jetstream options.
type JSServiceOption func(*KVJetStreamSvc) error

// GetAllKeys returns all the keys in the key-value store.
func (kv *KVJetStreamSvc) GetAllKeys(_ context.Context) ([]string, error) {
	keys, err := kv.kv.Keys()
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (kv *KVJetStreamSvc) GetValue(_ context.Context, k string) ([]byte, error) {
	kvEntry, err := kv.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := kvEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (kv *KVJetStreamSvc) GetStoreName() string {
	return kv.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (kv *KVJetStreamSvc) DeleteKey(_ context.Context, k string) error {
	return kv.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (kv *KVJetStreamSvc) PutKV(_ context.Context, k string, v []byte) error {
	_, err := kv.kv.Put(k, v)
	return err
}

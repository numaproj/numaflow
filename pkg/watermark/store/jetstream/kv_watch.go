package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	jsclient "github.com/numaproj/numaflow/pkg/isbsvc/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// KVJetStreamWatch implements the watermark's KV store backed up by Jetstream.
type KVJetStreamWatch struct {
	pipelineName string
	conn         *nats.Conn
	kv           nats.KeyValue
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*KVJetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to Jetsteam which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client jsclient.JetStreamClient, opts ...JSKVWatcherOption) (*KVJetStreamWatch, error) {
	var err error
	conn, err := client.Connect(ctx, jsclient.AutoReconnect())
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	// do we need to specify any opts? if yes, send it via options.
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j := &KVJetStreamWatch{
		pipelineName: pipelineName,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}

	j.kv, err = j.js.KeyValue(kvBucketName)
	if err != nil {
		return nil, err
	}

	// At this point, kvWatcher of type nats.KeyWatcher is nil

	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// JSKVWatcherOption is to pass in Jetstream options.
type JSKVWatcherOption func(*KVJetStreamWatch) error

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

// Watch watches the key-value store (aka bucket).
func (k *KVJetStreamWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	kvWatcher, err := k.kv.WatchAll()
	for err != nil {
		k.log.Errorw("WatchAll failed", zap.String("watcher", k.GetKVName()), zap.Error(err))
		kvWatcher, err = k.kv.WatchAll()
		time.Sleep(100 * time.Millisecond)
	}

	var updates = make(chan store.WatermarkKVEntry)
	go func() {
		for {
			select {
			case <-ctx.Done():
				k.log.Errorw("stopping WatchAll", zap.String("watcher", k.GetKVName()))
				// call jetstream watch stop
				_ = kvWatcher.Stop()
				return
			case value := <-kvWatcher.Updates():
				// if channel is closed, nil could come in
				if value == nil {
					continue
				}
				k.log.Info(value.Key(), value.Value(), value.Operation())
				switch value.Operation() {
				case nats.KeyValuePut:
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVPut,
					}
				case nats.KeyValueDelete:
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVDelete,
					}
				case nats.KeyValuePurge:
					// do nothing
				}
			}
		}
	}()
	return updates
}

// GetKVName returns the KV store (bucket) name.
func (k *KVJetStreamWatch) GetKVName() string {
	return k.kv.Bucket()
}

// Close closes the connection.
func (k *KVJetStreamWatch) Close() {
	if !k.conn.IsClosed() {
		k.conn.Close()
	}
}

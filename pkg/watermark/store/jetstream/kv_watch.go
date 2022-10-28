package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// jetStreamWatch implements the watermark's KV store backed up by Jetstream.
type jetStreamWatch struct {
	pipelineName string
	conn         *jsclient.NatsConn
	kv           nats.KeyValue
	js           *jsclient.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*jetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client jsclient.JetStreamClient, opts ...JSKVWatcherOption) (store.WatermarkKVWatcher, error) {
	var err error
	conn, err := client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	// do we need to specify any opts? if yes, send it via options.
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		if !conn.IsClosed() {
			conn.Close()
		}
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j := &jetStreamWatch{
		pipelineName: pipelineName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}

	j.kv, err = j.js.KeyValue(kvBucketName)
	if err != nil {
		if !conn.IsClosed() {
			conn.Close()
		}
		return nil, err
	}

	// At this point, kvWatcher of type nats.KeyWatcher is nil

	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			if !conn.IsClosed() {
				conn.Close()
			}
			return nil, err
		}
	}
	return j, nil
}

// JSKVWatcherOption is to pass in Jetstream options.
type JSKVWatcherOption func(*jetStreamWatch) error

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
func (k *jetStreamWatch) Watch(ctx context.Context) (<-chan store.WatermarkKVEntry, <-chan struct{}) {
	kvWatcher, err := k.kv.WatchAll()
	for err != nil {
		k.log.Errorw("WatchAll failed", zap.String("watcher", k.GetKVName()), zap.Error(err))
		kvWatcher, err = k.kv.WatchAll()
		time.Sleep(100 * time.Millisecond)
	}

	var updates = make(chan store.WatermarkKVEntry)
	var stopped = make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				k.log.Infow("stopping WatchAll", zap.String("watcher", k.GetKVName()))
				// call jetstream watch stop
				err = kvWatcher.Stop()
				if err != nil {
					k.log.Errorw("Failed to stop", zap.String("watcher", k.GetKVName()), zap.Error(err))
				} else {
					k.log.Infow("WatchAll successfully stopped", zap.String("watcher", k.GetKVName()))
				}
				close(updates)
				close(stopped)
				return
			case value := <-kvWatcher.Updates():
				// if channel is closed, nil could come in
				if value == nil {
					continue
				}
				k.log.Debug(value.Key(), value.Value(), value.Operation())
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
	return updates, stopped
}

// GetKVName returns the KV store (bucket) name.
func (k *jetStreamWatch) GetKVName() string {
	return k.kv.Bucket()
}

// Close closes the connection.
func (k *jetStreamWatch) Close() {
	// need to cancel the `Watch` ctx before calling Close()
	// otherwise `kvWatcher.Stop()` will raise the nats connection is closed error
	if !k.conn.IsClosed() {
		k.conn.Close()
	}
}

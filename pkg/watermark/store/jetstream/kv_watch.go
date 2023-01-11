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

package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// jetStreamWatch implements the watermark's KV store backed up by Jetstream.
type jetStreamWatch struct {
	pipelineName string
	kvBucketName string
	conn         *jsclient.NatsConn
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

	js, err := conn.JetStream()
	if err != nil {
		if !conn.IsClosed() {
			conn.Close()
		}
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j := &jetStreamWatch{
		pipelineName: pipelineName,
		kvBucketName: kvBucketName,
		conn:         conn,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}

	// At this point, kvWatcher of type nats.KeyWatcher is nil

	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			j.Close()
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
func (jsw *jetStreamWatch) Watch(ctx context.Context) (<-chan store.WatermarkKVEntry, <-chan struct{}) {
	var err error
	kvWatcher := jsw.newWatcher()
	var updates = make(chan store.WatermarkKVEntry)
	var stopped = make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				jsw.log.Infow("stopping WatchAll", zap.String("watcher", jsw.GetKVName()))
				// call JetStream watcher stop
				err = kvWatcher.Stop()
				if err != nil {
					jsw.log.Errorw("Failed to stop", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
				} else {
					jsw.log.Infow("WatchAll successfully stopped", zap.String("watcher", jsw.GetKVName()))
				}
				close(updates)
				close(stopped)
				return
			case value, ok := <-kvWatcher.Updates():
				if !ok {
					// there are no more values to receive and the channel is closed, but context is not done yet
					// meaning: there could be an auto reconnection to JetStream while the service is still running
					// therefore, recreate the kvWatcher using the new JetStream context
					kvWatcher = jsw.newWatcher()
					jsw.log.Infow("Succeeded to recreate the watcher")
				}
				if value == nil {
					// watcher initialization and subscription send nil value
					continue
				}
				jsw.log.Debug(value.Key(), value.Value(), value.Operation())
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

func (jsw *jetStreamWatch) newWatcher() nats.KeyWatcher {
	kv, err := jsw.js.KeyValue(jsw.kvBucketName)
	// keep looping because the watermark won't work without a watcher
	for err != nil {
		jsw.log.Errorw("Failed to bind to the JetStream KeyValue store", zap.String("kvBucketName", jsw.kvBucketName), zap.String("watcher", jsw.GetKVName()), zap.Error(err))
		kv, err = jsw.js.KeyValue(jsw.kvBucketName)
		time.Sleep(100 * time.Millisecond)
	}
	kvWatcher, err := kv.WatchAll(nats.IncludeHistory())
	// keep looping because the watermark won't work without a watcher
	for err != nil {
		jsw.log.Errorw("WatchAll failed", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
		kvWatcher, err = kv.WatchAll(nats.IncludeHistory())
		time.Sleep(100 * time.Millisecond)
	}
	return kvWatcher
}

// GetKVName returns the KV store (bucket) name.
func (jsw *jetStreamWatch) GetKVName() string {
	return jsw.kvBucketName
}

// Close closes the connection.
func (jsw *jetStreamWatch) Close() {
	// need to cancel the `Watch` ctx before calling Close()
	// otherwise `kvWatcher.Stop()` will raise the nats connection is closed error
	if !jsw.conn.IsClosed() {
		jsw.conn.Close()
	}
}

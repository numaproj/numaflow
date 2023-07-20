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
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// jetStreamWatch implements the watermark's KV store backed up by Jetstream.
type jetStreamWatch struct {
	pipelineName string
	kvBucketName string
	conn         *jsclient.NatsConn
	js           *jsclient.JetStreamContext
	kvWatchers   []*KeyWatcher
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*jetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client jsclient.JetStreamClient, opts ...JSKVWatcherOption) (store.WatermarkKVWatcher, error) {
	jsw := &jetStreamWatch{
		pipelineName: pipelineName,
		kvBucketName: kvBucketName,
		kvWatchers:   make([]*KeyWatcher, 0),
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}

	connectAndWatch := func() (*jsclient.NatsConn, *jsclient.JetStreamContext, error) {
		conn, err := client.Connect(ctx, jsclient.ReconnectHandler(func(c *jsclient.NatsConn) {
			// update the watcher reference to the new connection
			jsw.log.Info("Recreating kv watchers inside custom reconnect handler")
			// recreate the watchers
			for _, w := range jsw.kvWatchers {
				w.RefreshWatcher(jsw.newWatcher().kvw)
			}
		}), jsclient.DisconnectErrHandler(func(nc *jsclient.NatsConn, err error) {
			jsw.log.Errorw("Nats JetStream connection lost inside kv watcher", zap.Error(err))
		}), jsclient.AutoReconnectHandler(func(nc *nats.Conn) {
			// update the watcher reference to the new connection
			jsw.conn.Conn = nc
			// reload the contexts
			jsw.conn.ReloadContexts()
			jsw.log.Info("Recreating kv watchers inside auto reconnect handler")
			// recreate the watchers
			for _, w := range jsw.kvWatchers {
				w.RefreshWatcher(jsw.newWatcher().kvw)
			}
		}))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get nats connection, %w", err)
		}
		js, err := conn.JetStream()
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("failed to get jetstream context, %w", err)
		}
		return conn, js, nil
	}

	conn, js, err := connectAndWatch()

	if err != nil {
		return nil, err
	}

	jsw.conn = conn
	jsw.js = js
	// options if any
	for _, o := range opts {
		if err := o(jsw); err != nil {
			jsw.Close()
			return nil, err
		}
	}
	return jsw, nil
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
	jsw.kvWatchers = append(jsw.kvWatchers, kvWatcher)
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
					kvWatcher.RefreshWatcher(jsw.newWatcher().kvw)
					jsw.log.Infow("Succeeded to recreate the watcher, since the channel is closed")
				}
				if value == nil {
					jsw.log.Infow("watcher initialization and subscription got nil value")
					continue
				}
				switch value.Operation() {
				case nats.KeyValuePut:
					jsw.log.Debugw("Received a put event", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVPut,
					}
				case nats.KeyValueDelete:
					jsw.log.Debugw("Received a delete event", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVDelete,
					}
				}
			}
		}
	}()
	return updates, stopped
}

func (jsw *jetStreamWatch) newWatcher() *KeyWatcher {
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
	kw := &KeyWatcher{
		kvw: kvWatcher,
	}
	return kw
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

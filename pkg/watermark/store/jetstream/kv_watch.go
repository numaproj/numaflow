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
	client       *jsclient.NATSClient
	kvWatchers   []nats.KeyWatcher
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*jetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client *jsclient.NATSClient, opts ...JSKVWatcherOption) (store.WatermarkKVWatcher, error) {
	jsw := &jetStreamWatch{
		pipelineName: pipelineName,
		kvBucketName: kvBucketName,
		kvWatchers:   make([]nats.KeyWatcher, 0),
		client:       client,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}
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
	var timer = time.NewTicker(5 * time.Second)
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
				jsw.log.Debugw("Received a value from the watcher", zap.String("watcher", jsw.GetKVName()), zap.Any("value", value), zap.Bool("ok", ok))
				if !ok {
					// there are no more values to receive and the channel is closed, but context is not done yet
					// meaning: there could be an auto reconnection to JetStream while the service is still running
					// therefore, recreate the kvWatcher using the new JetStream context
					kvWatcher = jsw.newWatcher()
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
			case <-timer.C:
				jsw.log.Info("ticker ticked inside kv watcher")
			}
		}
	}()
	return updates, stopped
}

func (jsw *jetStreamWatch) newWatcher() nats.KeyWatcher {
	kvWatcher, err := jsw.client.CreateKVWatcher(jsw.kvBucketName)
	// keep looping because the watermark won't work without a watcher
	for err != nil {
		jsw.log.Errorw("Creating watcher failed", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
		kvWatcher, err = jsw.client.CreateKVWatcher(jsw.kvBucketName)
		time.Sleep(100 * time.Millisecond)
	}
	return kvWatcher
}

// GetKVName returns the KV store (bucket) name.
func (jsw *jetStreamWatch) GetKVName() string {
	return jsw.kvBucketName
}

// Close stops the watchers.
func (jsw *jetStreamWatch) Close() {
	// need to cancel the `Watch` ctx before calling Close()
	// otherwise `kvWatcher.Stop()` will raise the nats connection is closed error
	for _, w := range jsw.kvWatchers {
		_ = w.Stop()
	}
}

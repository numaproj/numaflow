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
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// jetStreamWatch implements the watermark's KV store backed up by Jetstream.
type jetStreamWatch struct {
	pipelineName string
	kvBucketName string
	client       *jsclient.NATSClient
	hbTimer      *time.Timer
	log          *zap.SugaredLogger
	opts         *options
}

var _ store.WatermarkKVWatcher = (*jetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client *jsclient.NATSClient, opts ...Option) (store.WatermarkKVWatcher, error) {

	kvOpts := defaultOptions()

	for _, o := range opts {
		o(kvOpts)
	}

	jsw := &jetStreamWatch{
		pipelineName: pipelineName,
		kvBucketName: kvBucketName,
		client:       client,
		hbTimer:      time.NewTimer(kvOpts.heartbeatThreshold),
		opts:         kvOpts,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("kvBucketName", kvBucketName),
	}
	return jsw, nil
}

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
	kvWatcher := jsw.newWatcher(ctx)
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
				jsw.log.Debugw("Received a value from the watcher", zap.String("watcher", jsw.GetKVName()), zap.Any("value", value), zap.Bool("ok", ok))
				if !ok {
					// there are no more values to receive and the channel is closed, but context is not done yet
					// meaning: there could be an auto reconnection to JetStream while the service is still running
					// therefore, recreate the kvWatcher using the new JetStream context
					tempWatcher := kvWatcher
					kvWatcher = jsw.newWatcher(ctx)
					err = tempWatcher.Stop()
					if err != nil {
						jsw.log.Warnw("Failed to stop the watcher", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
					}
					jsw.log.Infow("Succeeded to recreate the watcher, since the channel is closed")
					continue
				}
				if value == nil {
					jsw.log.Infow("watcher initialization and subscription got nil value")
					continue
				}
				// check if the key is a heartbeat key, if so, reset the hb timer
				if value.Key() == v1alpha1.KVHeartbeatKey {
					jsw.log.Debug("Received a heartbeat event", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
					if !jsw.hbTimer.Stop() {
						<-jsw.hbTimer.C
					}
					jsw.hbTimer.Reset(jsw.opts.heartbeatThreshold)
					continue
				}
				switch value.Operation() {
				case nats.KeyValuePut:
					jsw.log.Debug("Received a put event", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVPut,
					}
				case nats.KeyValueDelete:
					jsw.log.Debug("Received a delete event", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    store.KVDelete,
					}
				}
			case <-jsw.hbTimer.C:
				// if the timer expired, it means that the watcher is not receiving any kv heartbeats
				// recreate the watcher and stop the old one
				jsw.log.Warn("Heartbeat timer expired", zap.String("watcher", jsw.GetKVName()))
				jsw.log.Warn("Recreating the watcher")
				tempWatcher := kvWatcher
				kvWatcher = jsw.newWatcher(ctx)
				err = tempWatcher.Stop()
				jsw.hbTimer.Reset(jsw.opts.heartbeatThreshold)
			}
		}
	}()
	return updates, stopped
}

func (jsw *jetStreamWatch) newWatcher(ctx context.Context) nats.KeyWatcher {
	kvWatcher, err := jsw.client.CreateKVWatcher(jsw.kvBucketName, nats.Context(ctx))
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

// Close noop
func (jsw *jetStreamWatch) Close() {
}

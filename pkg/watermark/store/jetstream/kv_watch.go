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
	pipelineName      string
	kvBucketName      string
	client            *jsclient.NATSClient
	kvStore           nats.KeyValue
	previousFetchTime time.Time
	kvwTimer          *time.Timer
	log               *zap.SugaredLogger
	opts              *options
}

var _ store.WatermarkKVWatcher = (*jetStreamWatch)(nil)

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client *jsclient.NATSClient, opts ...Option) (store.WatermarkKVWatcher, error) {

	kvOpts := defaultOptions()

	for _, o := range opts {
		o(kvOpts)
	}

	kvStore, err := client.BindKVStore(kvBucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to bind kv store: %w", err)
	}

	jsw := &jetStreamWatch{
		pipelineName: pipelineName,
		kvBucketName: kvBucketName,
		client:       client,
		kvStore:      kvStore,
		kvwTimer:     time.NewTimer(kvOpts.watcherCreationThreshold),
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
	// create a new watcher, it will keep retrying until the context is done
	// returns nil if the context is done
	kvWatcher := jsw.newWatcher(ctx)
	var updates = make(chan store.WatermarkKVEntry)
	var stopped = make(chan struct{})
	go func() {
		// if kvWatcher is nil, it means the context is done
		for kvWatcher != nil {
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
				// we are getting updates from the watcher, reset the timer
				// drain the timer channel if it is not empty before resetting
				if !jsw.kvwTimer.Stop() {
					<-jsw.kvwTimer.C
				}
				jsw.kvwTimer.Reset(jsw.opts.watcherCreationThreshold)

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
				jsw.previousFetchTime = value.Created()

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
			case <-jsw.kvwTimer.C:
				// if the timer expired, it means that the watcher is not receiving any updates
				kvLastUpdatedTime := jsw.lastUpdateKVTime()
				// if the last update time is before the previous fetch time, it means that the store is not getting any updates
				// therefore, we don't have to recreate the watcher
				if kvLastUpdatedTime.Before(jsw.previousFetchTime) {
					jsw.log.Debug("The watcher is not receiving any updates, but the store is not getting any updates either", zap.String("watcher", jsw.GetKVName()), zap.Time("lastUpdateKVTime", kvLastUpdatedTime), zap.Time("previousFetchTime", jsw.previousFetchTime))
				} else {
					// if the last update time is after the previous fetch time, it means that the store is getting updates but the watcher is not receiving any
					// therefore, we have to recreate the watcher
					jsw.log.Warn("The watcher is not receiving any updates", zap.String("watcher", jsw.GetKVName()), zap.Time("lastUpdateKVTime", kvLastUpdatedTime), zap.Time("previousFetchTime", jsw.previousFetchTime))
					jsw.log.Warn("Recreating the watcher")
					tempWatcher := kvWatcher
					kvWatcher = jsw.newWatcher(ctx)
					err = tempWatcher.Stop()
				}
				// reset the timer, since we have drained the timer channel its safe to reset it
				jsw.kvwTimer.Reset(jsw.opts.watcherCreationThreshold)

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

// lastUpdateKVTime returns the last update time of the kv store
func (jsw *jetStreamWatch) lastUpdateKVTime() time.Time {
	var (
		keys       []string
		err        error
		lastUpdate time.Time
		value      nats.KeyValueEntry
	)

retryLoop:
	for {
		keys, err = jsw.kvStore.Keys()
		if err == nil {
			break retryLoop
		} else {
			// if there are no keys in the store, return zero time because there are no updates
			// upstream will handle it
			if err == nats.ErrNoKeysFound {
				return time.Time{}
			}
			jsw.log.Errorw("Failed to get keys", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, key := range keys {
		value, err = jsw.kvStore.Get(key)
		for err != nil {
			jsw.log.Errorw("Failed to get value", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
			value, err = jsw.kvStore.Get(key)
			time.Sleep(100 * time.Millisecond)
		}
		if value.Created().After(lastUpdate) {
			lastUpdate = value.Created()
		}
	}
	return lastUpdate
}

// GetKVName returns the KV store (bucket) name.
func (jsw *jetStreamWatch) GetKVName() string {
	return jsw.kvBucketName
}

// Close noop
func (jsw *jetStreamWatch) Close() {
}

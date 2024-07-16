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

/*
Package jetstream package implements the kv store and watcher using Jetstream.
*/
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// jetStreamStore implements the KV store backed up by Jetstream.
type jetStreamStore struct {
	ctx               context.Context
	kvName            string
	client            *jsclient.Client
	kv                nats.KeyValue
	previousFetchTime time.Time
	doneCh            chan struct{}

	log  *zap.SugaredLogger
	opts *options
}

var _ kvs.KVStorer = (*jetStreamStore)(nil)

// NewKVJetStreamKVStore returns KVJetStreamStore.
func NewKVJetStreamKVStore(ctx context.Context, kvName string, client *jsclient.Client, opts ...Option) (kvs.KVStorer, error) {
	var err error

	kvOpts := defaultOptions()

	for _, o := range opts {
		o(kvOpts)
	}

	kvStore, err := client.BindKVStore(kvName)

	if err != nil {
		return nil, fmt.Errorf("failed to bind kv store: %w", err)
	}

	var jsStore = &jetStreamStore{
		ctx:    ctx,
		kvName: kvName,
		kv:     kvStore,
		client: client,
		opts:   kvOpts,
		doneCh: make(chan struct{}),
		log:    logging.FromContext(ctx).With("kvName", kvName),
	}

	return jsStore, nil
}

// kvEntry is each key-value entry in the store and the operation associated with the kv pair.
type kvEntry struct {
	key   string
	value []byte
	op    kvs.KVWatchOp
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
func (k kvEntry) Operation() kvs.KVWatchOp {
	return k.op
}

// GetAllKeys returns all the keys in the key-value store.
func (jss *jetStreamStore) GetAllKeys(_ context.Context) ([]string, error) {
	keyLister, err := jss.kv.ListKeys()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = keyLister.Stop()
	}()

	var keys []string
	for key := range keyLister.Keys() {
		keys = append(keys, key)
	}
	return keys, nil
}

// GetValue returns the value for a given key.
func (jss *jetStreamStore) GetValue(_ context.Context, k string) ([]byte, error) {
	keyValueEntry, err := jss.kv.Get(k)
	if err != nil {
		return []byte(""), err
	}

	val := keyValueEntry.Value()
	return val, err
}

// GetStoreName returns the store name.
func (jss *jetStreamStore) GetStoreName() string {
	return jss.kv.Bucket()
}

// DeleteKey deletes the key from the JS key-value store.
func (jss *jetStreamStore) DeleteKey(_ context.Context, k string) error {
	// will return error if nats connection is closed
	return jss.kv.Delete(k)
}

// PutKV puts an element to the JS key-value store.
func (jss *jetStreamStore) PutKV(_ context.Context, k string, v []byte) error {
	// will return error if nats connection is closed
	_, err := jss.kv.Put(k, v)
	return err
}

func (jss *jetStreamStore) GetKVName() string {
	return jss.kvName
}

// Watch watches the key-value store (aka bucket) and returns the updates channel to read the updates on the KV store.
func (jss *jetStreamStore) Watch(ctx context.Context) <-chan kvs.KVEntry {
	var err error
	// create a new watcher, it will keep retrying until the context is done
	// returns nil if the context is done
	kvWatcher := jss.newWatcher(ctx)
	kvwTimer := time.NewTimer(jss.opts.watcherCreationThreshold)

	var updates = make(chan kvs.KVEntry)
	go func() {
		// if kvWatcher is nil, it means the context is done
		for kvWatcher != nil {
			select {
			case <-ctx.Done():
				jss.log.Infow("stopping WatchAll", zap.String("watcher", jss.GetKVName()))
				// call JetStream watcher stop
				err = kvWatcher.Stop()
				if err != nil {
					jss.log.Errorw("Failed to stop", zap.String("watcher", jss.GetKVName()), zap.Error(err))
				} else {
					jss.log.Infow("WatchAll successfully stopped", zap.String("watcher", jss.GetKVName()))
				}
				close(updates)
				return
			case value, ok := <-kvWatcher.Updates():
				// we are getting updates from the watcher, reset the timer
				// drain the timer channel if it is not empty before resetting
				if !kvwTimer.Stop() {
					<-kvwTimer.C
				}
				kvwTimer.Reset(jss.opts.watcherCreationThreshold)

				jss.log.Debugw("Received a value from the watcher", zap.String("watcher", jss.GetKVName()), zap.Any("value", value), zap.Bool("ok", ok))
				if !ok {
					// there are no more values to receive and the channel is closed, but context is not done yet
					// meaning: there could be an auto reconnection to JetStream while the service is still running
					// therefore, recreate the kvWatcher using the new JetStream context
					tempWatcher := kvWatcher
					kvWatcher = jss.newWatcher(ctx)
					err = tempWatcher.Stop()
					if err != nil {
						jss.log.Warnw("Failed to stop the watcher", zap.String("watcher", jss.GetKVName()), zap.Error(err))
					}
					jss.log.Infow("Succeeded to recreate the watcher, since the channel is closed")
					continue
				}
				if value == nil {
					jss.log.Infow("Watcher initialization and subscription got nil value")
					continue
				}
				jss.previousFetchTime = value.Created()

				switch value.Operation() {
				case nats.KeyValuePut:
					jss.log.Debugw("Received a put event", zap.String("key", value.Key()), zap.Binary("b64Value", value.Value()))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    kvs.KVPut,
					}
				case nats.KeyValueDelete:
					jss.log.Infow("Received a delete event", zap.String("key", value.Key()), zap.Binary("b64Value", value.Value()))
					updates <- kvEntry{
						key:   value.Key(),
						value: value.Value(),
						op:    kvs.KVDelete,
					}
				}
			case <-kvwTimer.C:
				// if the timer expired, it means that the watcher is not receiving any updates
				kvLastUpdatedTime := jss.lastUpdateKVTime()

				// if the last update time is zero, it means that there are no key-value pairs in the store yet or ctx was canceled both the cases we should not recreate the watcher
				// if the last update time is not after the previous fetch time, it means that the store is not getting any updates (watermark is not getting updated)
				// therefore, we don't have to recreate the watcher
				if kvLastUpdatedTime.IsZero() || !kvLastUpdatedTime.After(jss.previousFetchTime) {
					jss.log.Debug("The watcher is not receiving any updates, but the store is not getting any updates either", zap.String("watcher", jss.GetKVName()), zap.Time("lastUpdateKVTime", kvLastUpdatedTime), zap.Time("previousFetchTime", jss.previousFetchTime))
				} else {
					// if the last update time is after the previous fetch time, it means that the store is getting updates but the watcher is not receiving any
					// therefore, we have to recreate the watcher
					jss.log.Warn("The watcher is not receiving any updates, recreating the watcher", zap.String("watcher", jss.GetKVName()), zap.Time("lastUpdateKVTime", kvLastUpdatedTime), zap.Time("previousFetchTime", jss.previousFetchTime))
					tempWatcher := kvWatcher
					kvWatcher = jss.newWatcher(ctx)
					err = tempWatcher.Stop()
				}
				// reset the timer, since we have drained the timer channel its safe to reset it
				kvwTimer.Reset(jss.opts.watcherCreationThreshold)

			case <-jss.doneCh:
				jss.log.Infow("Stopping WatchAll", zap.String("watcher", jss.GetKVName()))
				close(updates)
				return
			}
		}
	}()
	return updates
}

// newWatcher creates a new watcher for the key-value store.
func (jss *jetStreamStore) newWatcher(ctx context.Context) nats.KeyWatcher {
	kvWatcher, err := jss.kv.WatchAll(nats.Context(ctx))
	// keep looping because the watermark won't work without a watcher
	for err != nil {
		select {
		case <-jss.ctx.Done():
			return nil
		default:
			jss.log.Errorw("Creating watcher failed", zap.String("watcher", jss.GetKVName()), zap.Error(err))
			kvWatcher, err = jss.kv.WatchAll(nats.Context(ctx))
			time.Sleep(100 * time.Millisecond)
		}
	}
	jss.log.Infow("Successfully created watcher", zap.String("watcher", jss.GetKVName()))
	return kvWatcher
}

// lastUpdateKVTime returns the last update time of the kv store. if the key is missing, it will return time.Zero.
func (jss *jetStreamStore) lastUpdateKVTime() time.Time {
	var (
		err        error
		lastUpdate time.Time
		keys       []string
		value      nats.KeyValueEntry
	)

retryLoop:
	for {
		select {
		case <-jss.ctx.Done():
			return time.Time{}
		default:
			keys, err = jss.GetAllKeys(jss.ctx)
			if err == nil {
				break retryLoop
			} else {
				// if there are no keys in the store, return zero time because there are no updates
				// upstream will handle it
				if errors.Is(err, nats.ErrNoKeysFound) {
					return time.Time{}
				}
				jss.log.Errorw("Failed to get keys", zap.String("watcher", jss.GetKVName()), zap.Error(err))
			}
			time.Sleep(100 * time.Millisecond)
		}

	}

outer:
	for _, key := range keys {
		value, err = jss.kv.Get(key)
		for err != nil {
			// keys can be deleted when the previous vertex pod is deleted/restarted.
			if errors.Is(err, nats.ErrKeyNotFound) {
				jss.log.Infow("Nats key not found", zap.String("watcher", jss.GetKVName()), zap.String("key", key))
				break outer
			}
			select {
			case <-jss.ctx.Done():
				return time.Time{}
			default:
				jss.log.Errorw("Failed to get value", zap.String("watcher", jss.GetKVName()), zap.String("key", key), zap.Error(err))
				time.Sleep(100 * time.Millisecond)
				value, err = jss.kv.Get(key)
			}
		}
		if value.Created().After(lastUpdate) {
			lastUpdate = value.Created()
		}
	}

	return lastUpdate
}

// Close we don't need to close the JetStream connection. It will be closed by the caller.
// give the signal to watchers to stop watching
func (jss *jetStreamStore) Close() {
	close(jss.doneCh)
}

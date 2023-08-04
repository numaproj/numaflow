package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store"
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

var _ store.SideInputWatcher = (*jetStreamWatch)(nil)

// SideInputEntry is each key-value entry in the store and the operation associated with the kv pair.
type SideInputEntry struct {
	key   string
	value []byte
}

// Key returns the key
func (s SideInputEntry) Key() string {
	return s.key
}

// Value returns the value.
func (s SideInputEntry) Value() []byte {
	return s.value
}

func (j *jetStreamWatch) Watch(ctx context.Context) (<-chan store.SideInputKVEntry, <-chan struct{}) {
	var err error
	kvWatcher := j.newWatcher(ctx)
	var updates = make(chan store.SideInputKVEntry)
	var stopped = make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				j.log.Infow("stopping WatchAll", zap.String("watcher", j.GetKVName()))
				// call JetStream watcher stop
				err = kvWatcher.Stop()
				if err != nil {
					j.log.Errorw("Failed to stop", zap.String("watcher", j.GetKVName()), zap.Error(err))
				} else {
					j.log.Infow("WatchAll successfully stopped", zap.String("watcher", j.GetKVName()))
				}
				close(updates)
				close(stopped)
				return
			case value, ok := <-kvWatcher.Updates():
				// we are getting updates from the watcher, reset the timer
				// drain the timer channel if it is not empty before resetting
				if !j.kvwTimer.Stop() {
					<-j.kvwTimer.C
				}
				j.kvwTimer.Reset(j.opts.watcherCreationThreshold)

				j.log.Debugw("Received a value from the watcher", zap.String("watcher", j.GetKVName()),
					zap.Any("value", value), zap.Bool("ok", ok))
				if !ok {
					// there are no more values to receive and the channel is closed, but context is not done yet
					// meaning: there could be an auto reconnection to JetStream while the service is still running
					// therefore, recreate the kvWatcher using the new JetStream context
					tempWatcher := kvWatcher
					kvWatcher = j.newWatcher(ctx)
					err = tempWatcher.Stop()
					if err != nil {
						j.log.Warnw("Failed to stop the watcher", zap.String("watcher", j.GetKVName()), zap.Error(err))
					}
					j.log.Infow("Succeeded to recreate the watcher, since the channel is closed")
					continue
				}
				if value == nil {
					j.log.Infow("watcher initialization and subscription got nil value")
					continue
				}
				j.previousFetchTime = value.Created()
				j.log.Debug("Received a value ", zap.String("key", value.Key()), zap.String("value", string(value.Value())))
				updates <- SideInputEntry{
					key:   value.Key(),
					value: value.Value(),
				}
			case <-j.kvwTimer.C:
				// if the timer expired, it means that the watcher is not receiving any updates
				kvLastUpdatedTime := j.lastUpdateKVTime()
				// if the last update time is before the previous fetch time, it means that the store is not getting any updates
				// therefore, we don't have to recreate the watcher
				if kvLastUpdatedTime.Before(j.previousFetchTime) {
					j.log.Debug("The watcher is not receiving any updates, but the store is not getting any updates either",
						zap.String("watcher", j.GetKVName()), zap.Time("lastUpdateKVTime", kvLastUpdatedTime),
						zap.Time("previousFetchTime", j.previousFetchTime))
				} else {
					// if the last update time is after the previous fetch time, it means that the store is getting updates but the watcher is not receiving any
					// therefore, we have to recreate the watcher
					j.log.Warn("The watcher is not receiving any updates", zap.String("watcher", j.GetKVName()),
						zap.Time("lastUpdateKVTime", kvLastUpdatedTime), zap.Time("previousFetchTime", j.previousFetchTime))
					j.log.Warn("Recreating the watcher")
					tempWatcher := kvWatcher
					kvWatcher = j.newWatcher(ctx)
					err = tempWatcher.Stop()
				}
				// reset the timer, since we have drained the timer channel its safe to reset it
				j.kvwTimer.Reset(j.opts.watcherCreationThreshold)

			}
		}
	}()
	return updates, stopped
}

func (j *jetStreamWatch) newWatcher(ctx context.Context) nats.KeyWatcher {
	kvWatcher, err := j.client.CreateKVWatcher(j.kvBucketName, nats.Context(ctx))
	// keep looping because the side-inputs won't work without a watcher
	for err != nil {
		j.log.Errorw("Creating watcher failed", zap.String("watcher", j.GetKVName()), zap.Error(err))
		kvWatcher, err = j.client.CreateKVWatcher(j.kvBucketName)
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("Creating watcher success", kvWatcher.Context())
	return kvWatcher
}

// lastUpdateKVTime returns the last update time of the kv store
func (jsw *jetStreamWatch) lastUpdateKVTime() time.Time {
	keys, err := jsw.kvStore.Keys()
	for err != nil {
		jsw.log.Errorw("Failed to get keys", zap.String("watcher", jsw.GetKVName()), zap.Error(err))
		keys, err = jsw.kvStore.Keys()
		time.Sleep(100 * time.Millisecond)
	}
	var lastUpdate = time.Time{}
	for _, key := range keys {
		value, err := jsw.kvStore.Get(key)
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

func (j *jetStreamWatch) GetKVName() string {
	//TODO implement me
	return j.kvBucketName
}

func (j *jetStreamWatch) Close() {
	//TODO implement me
	panic("implement me")
}

// NewKVJetStreamKVWatch returns KVJetStreamWatch specific to JetStream which implements the WatermarkKVWatcher interface.
func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, kvBucketName string, client *jsclient.NATSClient, opts ...Option) (store.SideInputWatcher, error) {

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

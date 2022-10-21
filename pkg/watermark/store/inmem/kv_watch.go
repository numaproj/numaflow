package inmem

import (
	"context"
	"sync"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// inMemWatch implements the watermark's KV store backed up by in memory store.
type inMemWatch struct {
	pipelineName string
	bucketName   string
	kvEntryCh    <-chan store.WatermarkKVEntry
	kvHistory    []store.WatermarkKVEntry
	updatesChMap map[string]chan store.WatermarkKVEntry
	lock         sync.Mutex // the lock for kvHistory and updatesChMap
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*inMemWatch)(nil)

// NewInMemWatch returns inMemWatch which implements the WatermarkKVWatcher interface.
func NewInMemWatch(ctx context.Context, pipelineName string, bucketName string, kvEntryCh <-chan store.WatermarkKVEntry) (store.WatermarkKVWatcher, error) {
	k := &inMemWatch{
		pipelineName: pipelineName,
		bucketName:   bucketName,
		kvEntryCh:    kvEntryCh,
		kvHistory:    []store.WatermarkKVEntry{},
		updatesChMap: make(map[string]chan store.WatermarkKVEntry),
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}
	return k, nil
}

// Watch watches the key-value store.
func (k *inMemWatch) Watch(ctx context.Context) (<-chan store.WatermarkKVEntry, <-chan struct{}) {
	// create a new updates channel and fill in the history
	var id = util.RandomString(10)
	var updates = make(chan store.WatermarkKVEntry)
	var stopped = make(chan struct{})

	// for new updates channel initialization
	go func() {
		k.lock.Lock()
		for _, value := range k.kvHistory {
			updates <- value
		}
		// add the new updates channel to the map before unlock
		// so the new updates channel won't miss new kv operations
		k.updatesChMap[id] = updates
		k.lock.Unlock()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				k.log.Infow("stopping watching", zap.String("watcher", k.GetKVName()))
				k.lock.Lock()
				delete(k.updatesChMap, id)
				close(updates)
				k.lock.Unlock()
				close(stopped)
				return
			case value := <-k.kvEntryCh:
				k.log.Debug(value.Key(), value.Value(), value.Operation())
				k.lock.Lock()
				k.kvHistory = append(k.kvHistory, value)
				for _, ch := range k.updatesChMap {
					ch <- value
				}
				k.lock.Unlock()
			}
		}
	}()
	return updates, stopped
}

// GetKVName returns the KV store (bucket) name.
func (k *inMemWatch) GetKVName() string {
	return k.bucketName
}

// Close does nothing here because the udpates channel has already been closed when exiting the Watch.
func (k *inMemWatch) Close() {
}

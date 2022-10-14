package inmem

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/shared/logging"
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// Watch watches the key-value store.
func (k *inMemWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	// create a new updates channel and fill in the history
	rand.Seed(time.Now().UnixNano())
	var id = randSeq(10)
	var updates = make(chan store.WatermarkKVEntry)

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
				k.log.Errorw("stopping watching", zap.String("watcher", k.GetKVName()))
				k.lock.Lock()
				delete(k.updatesChMap, id)
				close(updates)
				k.lock.Unlock()
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
	return updates
}

// GetKVName returns the KV store (bucket) name.
func (k *inMemWatch) GetKVName() string {
	return k.bucketName
}

// Close closes the connection.
func (k *inMemWatch) Close() {
}

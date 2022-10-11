package inmem

import (
	"context"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// inMemWatch implements the watermark's KV store backed up by in memory store.
type inMemWatch struct {
	pipelineName string
	bucketName   string
	kvEntryCh    <-chan store.WatermarkKVEntry
	stopCh       chan struct{}
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*inMemWatch)(nil)

// NewInMemWatch returns inMemWatch which implements the WatermarkKVWatcher interface.
func NewInMemWatch(ctx context.Context, pipelineName string, bucketName string, kvEntryCh <-chan store.WatermarkKVEntry, opts ...inMemStoreWatcherOption) (store.WatermarkKVWatcher, error) {

	j := &inMemWatch{
		pipelineName: pipelineName,
		bucketName:   bucketName,
		kvEntryCh:    kvEntryCh,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// inMemStoreWatcherOption is to pass in options.
type inMemStoreWatcherOption func(*inMemWatch) error

// Watch watches the key-value store.
func (k *inMemWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	var updates = make(chan store.WatermarkKVEntry)
	go func() {
		for {
			select {
			case <-ctx.Done():
				k.log.Errorw("stopping watching", zap.String("watcher", k.GetKVName()))
				close(k.stopCh)
				return
			case value := <-k.kvEntryCh:
				k.log.Debug(value.Key(), value.Value(), value.Operation())
				updates <- value
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
	return
}

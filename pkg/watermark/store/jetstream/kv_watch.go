package jetstream

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// KVJetStreamWatch implements the watermark's KV store backed up by Jetstream.
type KVJetStreamWatch struct {
	pipelineName string
	conn         *nats.Conn
	kv           nats.KeyValue
	js           nats.JetStreamContext
	log          *zap.SugaredLogger
}

var _ store.WatermarkKVWatcher = (*KVJetStreamWatch)(nil)

func NewKVJetStreamKVWatch(ctx context.Context, pipelineName string, bucketName string, client clients.JetStreamClient, opts ...JSKVWatcherOption) (*KVJetStreamWatch, error) {
	var err error
	conn, err := client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	// do we need to specify any opts? if yes, send it via options.
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	j := &KVJetStreamWatch{
		pipelineName: pipelineName,
		js:           js,
		log:          logging.FromContext(ctx).With("pipeline", pipelineName).With("bucketName", bucketName),
	}

	j.kv, err = j.js.KeyValue(bucketName)
	if err != nil {
		return nil, err
	}

	// At this point, kvWatcher of type nats.KeyWatcher is nil

	// options if any
	for _, o := range opts {
		if err := o(j); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// JSKVWatcherOption is to pass in Jetstream options.
type JSKVWatcherOption func(*KVJetStreamWatch) error

type kvEntry struct {
	key   string
	value []byte
	op    store.KVWatchOp
}

func (k kvEntry) Key() string {
	return k.key
}

func (k kvEntry) Value() []byte {
	return k.value
}

func (k kvEntry) Operation() store.KVWatchOp {
	return k.op
}

func (k *KVJetStreamWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	kvWatcher, err := k.kv.WatchAll()
	for err != nil {
		kvWatcher, err = k.kv.WatchAll()
	}

	var updates = make(chan store.WatermarkKVEntry)
	go func() {
		for true {
			select {
			case <-ctx.Done():
				fmt.Println("STOPPED")
				_ = kvWatcher.Stop()
				return
			case value := <-kvWatcher.Updates():
				// if channel is closed, nil could come in
				if value == nil {
					continue
				}
				k.log.Info(value.Key(), value.Value(), value.Operation())
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
	return updates
}

func (k *KVJetStreamWatch) GetKVName() string {
	return k.kv.Bucket()
}

func (k *KVJetStreamWatch) Stop(_ context.Context) {
	if !k.conn.IsClosed() {
		k.conn.Close()
	}
}

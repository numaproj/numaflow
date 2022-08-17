package noop

import (
	"context"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

type KVNoOpWatch struct {
}

var _ store.WatermarkKVWatcher = (*KVNoOpWatch)(nil)

func NewKVOpWatch() *KVNoOpWatch {
	return &KVNoOpWatch{}
}

// Watch returns a blocking channel.
func (K KVNoOpWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	retChan := make(chan store.WatermarkKVEntry)
	return retChan
}

func (K KVNoOpWatch) GetKVName() string {
	return ""
}

// Close closes, but we do not close the channel created during watch here; that should be taken care of by the context done
func (K KVNoOpWatch) Close() {
}

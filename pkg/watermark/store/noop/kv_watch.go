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

// Watch We return a nil channel as we do not wish to progress the watermark
func (K KVNoOpWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	retChan := make(chan store.WatermarkKVEntry)
	return retChan
}

func (K KVNoOpWatch) GetKVName() string {
	return ""
}

// Close We do not close the channel created during watch here, that should be taken care by the context done
func (K KVNoOpWatch) Close() {
}

package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/store"
)

type noOpWatch struct {
}

var _ store.WatermarkKVWatcher = (*noOpWatch)(nil)

func NewKVOpWatch() store.WatermarkKVWatcher {
	return &noOpWatch{}
}

// Watch returns a blocking channel.
func (no noOpWatch) Watch(ctx context.Context) <-chan store.WatermarkKVEntry {
	retChan := make(chan store.WatermarkKVEntry)
	return retChan
}

func (no noOpWatch) GetKVName() string {
	return ""
}

// Close closes, but we do not close the channel created during watch here; that should be taken care of by the context done
func (no noOpWatch) Close() {
}

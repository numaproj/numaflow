// Package noop implements a noop KV store and watch for watermark progression.

package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// KVNoOpStore does a no-op store.
type KVNoOpStore struct {
}

var _ store.WatermarkKVStorer = (*KVNoOpStore)(nil)

func NewKVNoOpStore() *KVNoOpStore {
	return &KVNoOpStore{}
}

func (K KVNoOpStore) GetAllKeys(_ context.Context) ([]string, error) {
	return []string{}, nil
}

func (K KVNoOpStore) DeleteKey(_ context.Context, _ string) error {
	return nil
}

func (K KVNoOpStore) PutKV(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (K KVNoOpStore) GetValue(_ context.Context, _ string) ([]byte, error) {
	return []byte{}, nil
}

func (K KVNoOpStore) GetStoreName() string {
	return "noop"
}

func (K KVNoOpStore) Close() {
}

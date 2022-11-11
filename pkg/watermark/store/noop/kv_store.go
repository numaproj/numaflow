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

// Package noop implements a noop KV store and watch for watermark progression.

package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// noOpStore is a no-op store which does not do any operation but can be safely invoked.
type noOpStore struct {
}

var _ store.WatermarkKVStorer = (*noOpStore)(nil)

// NewKVNoOpStore returns a no-op WatermarkKVStorer.
func NewKVNoOpStore() store.WatermarkKVStorer {
	return &noOpStore{}
}

func (k noOpStore) GetAllKeys(_ context.Context) ([]string, error) {
	return []string{}, nil
}

func (k noOpStore) DeleteKey(_ context.Context, _ string) error {
	return nil
}

func (k noOpStore) PutKV(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (k noOpStore) GetValue(_ context.Context, _ string) ([]byte, error) {
	return []byte{}, nil
}

func (k noOpStore) GetStoreName() string {
	return "noop"
}

func (k noOpStore) Close() {
}

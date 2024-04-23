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

package noop

import (
	"context"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/window"
)

// noopCompactor is a no-op compactor which does not do any operation but can be safely invoked.
type noopCompactor struct {
}

// NewNoopCompactor returns a new no-op compactor
func NewNoopCompactor() unaligned.Compactor {
	return &noopCompactor{}
}

func (n noopCompactor) Start(ctx context.Context) error {
	return nil
}

func (n noopCompactor) Stop() error {
	return nil
}

// noopGCEventsWAL is a no-op gc events WAL which does not do any operation but can be safely invoked.
type noopGCEventsWAL struct {
}

// NewNoopGCEventsWAL returns a new no-op GCEventsWAL
func NewNoopGCEventsWAL() unaligned.GCEventsWAL {
	return &noopGCEventsWAL{}
}

func (n noopGCEventsWAL) PersistGCEvent(window window.TimedWindow) error {
	return nil
}

func (n noopGCEventsWAL) Close() error {
	return nil
}

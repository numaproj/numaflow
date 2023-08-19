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

package store

import "github.com/numaproj/numaflow/pkg/shared/kvs"

// watermarkStore wraps a pair of heartbeatStore and offsetTimelineStore,
// it implements interface WatermarkStore.
type watermarkStore struct {
	heartbeatStore      kvs.KVStorer
	offsetTimelineStore kvs.KVStorer
}

var _ WatermarkStore = (*watermarkStore)(nil)

// BuildWatermarkStore returns a WatermarkStore instance
func BuildWatermarkStore(hbStore, otStore kvs.KVStorer) WatermarkStore {
	return &watermarkStore{
		heartbeatStore:      hbStore,
		offsetTimelineStore: otStore,
	}
}

func (ws *watermarkStore) HeartbeatStore() kvs.KVStorer {
	return ws.heartbeatStore
}

func (ws *watermarkStore) OffsetTimelineStore() kvs.KVStorer {
	return ws.offsetTimelineStore
}

func (ws *watermarkStore) Close() error {
	ws.heartbeatStore.Close()
	ws.offsetTimelineStore.Close()
	return nil
}

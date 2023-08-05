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

// watermarkStoreWatcher defines a pair of heartbeatStoreWatcher and offsetTimelineStoreWatcher,
// it implements interface WatermarkStoreWatcher
type watermarkStoreWatcher struct {
	heartbeatStoreWatcher      kvs.KVWatcher
	offsetTimelineStoreWatcher kvs.KVWatcher
}

var _ WatermarkStoreWatcher = (*watermarkStoreWatcher)(nil)

func (w *watermarkStoreWatcher) HeartbeatWatcher() kvs.KVWatcher {
	return w.heartbeatStoreWatcher
}
func (w *watermarkStoreWatcher) OffsetTimelineWatcher() kvs.KVWatcher {
	return w.offsetTimelineStoreWatcher
}

// BuildWatermarkStoreWatcher returns a WatermarkStoreWatcher instance
func BuildWatermarkStoreWatcher(hbStoreWatcher, otStoreWatcher kvs.KVWatcher) WatermarkStoreWatcher {
	return &watermarkStoreWatcher{
		heartbeatStoreWatcher:      hbStoreWatcher,
		offsetTimelineStoreWatcher: otStoreWatcher,
	}
}

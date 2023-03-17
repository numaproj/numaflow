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

// Package keyed implements KeyedWindows. A keyed window associates key(s) with a window.
// A key uniquely identifies a partitioned set of events in a given time window.
package keyed

import (
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// AlignedKeyedWindow maintains association between slots and a window.
// In a keyed stream, we need to close all the partitions when the watermark is past the window.
type AlignedKeyedWindow struct {
	// Start time of the window
	Start time.Time
	// End time of the window
	End time.Time
	// slots map of slots
	slots map[string]struct{}
	lock  sync.RWMutex
}

var _ window.AlignedKeyedWindower = (*AlignedKeyedWindow)(nil)

// NewKeyedWindow creates a new keyed window
func NewKeyedWindow(start time.Time, end time.Time) *AlignedKeyedWindow {
	kw := &AlignedKeyedWindow{
		Start: start,
		End:   end,
		slots: make(map[string]struct{}),
		lock:  sync.RWMutex{},
	}
	return kw
}

// StartTime returns start of the window.
func (kw *AlignedKeyedWindow) StartTime() time.Time {
	return kw.Start
}

// EndTime returns end of the window.
func (kw *AlignedKeyedWindow) EndTime() time.Time {
	return kw.End
}

// AddSlot adds a slot to an existing window
func (kw *AlignedKeyedWindow) AddSlot(slot string) {
	kw.lock.Lock()
	defer kw.lock.Unlock()
	if _, ok := kw.slots[slot]; !ok {
		kw.slots[slot] = struct{}{}
	}
}

// Partitions returns an array of partitions for a window
func (kw *AlignedKeyedWindow) Partitions() []partition.ID {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	partitions := make([]partition.ID, len(kw.slots))
	idx := 0
	for k := range kw.slots {
		partitions[idx] = partition.ID{Start: kw.StartTime(), End: kw.EndTime(), Slot: k}
		idx++
	}

	return partitions
}

func (kw *AlignedKeyedWindow) Slots() []string {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	keys := make([]string, len(kw.slots))
	idx := 0
	for k := range kw.slots {
		keys[idx] = k
		idx++
	}

	return keys
}

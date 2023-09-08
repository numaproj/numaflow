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

package wmb

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

// IdleManager manages the idle watermark whether the control message is a duplicate and also keeps track of the idle WMB's offset.
type IdleManager struct {
	// wmbOffset is a toBuffer partition name to the write offset of the idle watermark map.
	wmbOffset map[string]isb.Offset
	lock      sync.RWMutex
}

// NewIdleManager returns an IdleManager object to track the watermark idle status.
func NewIdleManager(length int) *IdleManager {
	return &IdleManager{
		wmbOffset: make(map[string]isb.Offset, length),
	}
}

// Validate returns true if the given partition hasn't got any control message and needs to create a new control message
func (im *IdleManager) NeedToSendCtrlMsg(toBufferPartitionName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()
	// if the given partition doesn't have a control message
	// the map entry will be empty, return true
	return im.wmbOffset[toBufferPartitionName] == nil
}

// Get gets the offset for the given toBuffer partition name.
func (im *IdleManager) Get(toBufferPartitionName string) isb.Offset {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferPartitionName]
}

// Update will update the existing item or add if not present for the given toBuffer partition name.
func (im *IdleManager) Update(toBufferPartitionName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferPartitionName] = newOffset
}

// Reset will clear the item for the given toBuffer partition name.
func (im *IdleManager) Reset(toBufferPartitionName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferPartitionName] = nil
}

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
	// wmbOffset is a toBufferName to the write offset of the idle watermark map.
	wmbOffset map[string]isb.Offset
	lock      sync.RWMutex
}

// NewIdleManager returns an IdleManager object to track the watermark idle status.
func NewIdleManager(length int) *IdleManager {
	return &IdleManager{
		wmbOffset: make(map[string]isb.Offset, length),
	}
}

// Exists returns true if the given toBuffer name exists in the IdleManager map.
func (im *IdleManager) Exists(toBufferName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferName] != nil
}

// Get gets the offset for the given toBufferName.
func (im *IdleManager) Get(toBufferName string) isb.Offset {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferName]
}

// Update will update the existing item or add if not present for the given toBuffer name.
func (im *IdleManager) Update(toBufferName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferName] = newOffset
}

// Reset will clear the item for the given toBuffer name.
func (im *IdleManager) Reset(toBufferName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferName] = nil
}

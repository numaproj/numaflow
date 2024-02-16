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
	"fmt"
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

// sharedIdleManager manages the idle watermark whether the control message is a duplicate and also keeps track of the idle WMB's offset.
// sharedIdleManager is shared across multiple forwarders. ATM our map and sink use sharedIdleManager.
type sharedIdleManager struct {
	// wmbOffset is a toBuffer partition name to the write offset of the idle watermark map.
	wmbOffset map[string]isb.Offset
	// forwarderIdlePartition is a map[toPartitionName]int to record the activeToPartitions
	// for the given forwarder.
	forwarderActiveToPartition map[string]int64
	lock                       sync.RWMutex
}

// NewSharedIdleManager returns an sharedIdleManager object as the IdleManager interface type to track the watermark idle status.
func NewSharedIdleManager(numOfFromPartitions int, numOfToPartitions int) (IdleManager, error) {
	if numOfFromPartitions <= 1 {
		return nil, fmt.Errorf("failed to create a new shared idle manager: numOfFromPartitions should be > 1")
	}
	return &sharedIdleManager{
		wmbOffset:                  make(map[string]isb.Offset, numOfToPartitions),
		forwarderActiveToPartition: make(map[string]int64, numOfToPartitions),
	}, nil
}

// NeedToSendCtrlMsg returns true if the given partition hasn't got any control message and needs to create a new control message
func (im *sharedIdleManager) NeedToSendCtrlMsg(toBufferPartitionName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()
	if im.forwarderActiveToPartition[toBufferPartitionName] != 0 {
		// only send ctrl msg when all bits are 0 (0 means inactive)
		return false
	}
	// if the given partition doesn't have a control message
	// the map entry will be empty, return true
	return im.wmbOffset[toBufferPartitionName] == nil
}

// Get gets the offset for the given toBuffer partition name.
func (im *sharedIdleManager) Get(toBufferPartitionName string) isb.Offset {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferPartitionName]
}

// Update will update the existing item or add if not present for the given toBuffer partition name.
func (im *sharedIdleManager) Update(fromBufferPartitionIndex int32, toBufferPartitionName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// clear means the fromBufferPartition forwarder is inactive as the default value is 0
	im.forwarderActiveToPartition[toBufferPartitionName] = clearBit(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex))
	im.wmbOffset[toBufferPartitionName] = newOffset
}

// Reset will clear the item for the given toBuffer partition name.
func (im *sharedIdleManager) Reset(fromBufferPartitionIndex int32, toBufferPartitionName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// set to 1 means mark the fromBufferPartition forwarder as active
	im.forwarderActiveToPartition[toBufferPartitionName] = setBit(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex))
	im.wmbOffset[toBufferPartitionName] = nil
}

// setBit sets the bit at pos in the integer n.
func setBit(n int64, pos uint) int64 {
	n |= 1 << pos
	return n
}

// clearBit clears the bit at pos in n.
func clearBit(n int64, pos uint) int64 {
	n &^= 1 << pos
	return n
}

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

// idleManager manages the idle watermark whether the control message is a duplicate and also keeps track of the idle WMB's offset.
type idleManager struct {
	// forwarderActivePartition is a map[toPartitionName]uint64 to record if a forwarder is sending to the toPartition.
	// for each "toPartition" we have an integer represents in binary, and mark the #n bit as 1 if the #n forwarder
	// is sending to the "toPartition"
	// example:
	//   if we have three forwarders, the initial value in binary format will be {"toPartition": 000}, which is 0 in decimal
	//   if forwarder0 is sending data to the toPartition, then the value will become {"toPartition": 001}
	//   if forwarder1 is sending data to the toPartition, then the value will become {"toPartition": 010}
	//   if forwarder2 is sending data to the toPartition, then the value will become {"toPartition": 100}
	// when we do the ctrlMsg check, we rely on the value (0 or non-0) to decide if we need to send a ctrlMsg
	// note that we use uint64, therefore there is a limit of maximum 64 partitions
	forwarderActiveToPartition map[string]uint64
	// wmbOffset is a toBuffer partition name to the write offset of the idle watermark map.
	wmbOffset map[string]isb.Offset
	lock      sync.RWMutex
}

// NewIdleManager returns an idleManager object as the IdleManager interface type to track the watermark idle status.
func NewIdleManager(numOfFromPartitions int, numOfToPartitions int) (IdleManager, error) {
	if numOfFromPartitions <= 0 || numOfFromPartitions >= 64 {
		return nil, fmt.Errorf("failed to create a new idle manager: numOfFromPartitions should be > 0 and < 64")
	}
	return &idleManager{
		forwarderActiveToPartition: make(map[string]uint64, numOfToPartitions),
		wmbOffset:                  make(map[string]isb.Offset, numOfToPartitions),
	}, nil
}

// NeedToSendCtrlMsg returns true if the given partition hasn't got any control message and needs to create a new control message
func (im *idleManager) NeedToSendCtrlMsg(toBufferPartitionName string) bool {
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
func (im *idleManager) Get(toBufferPartitionName string) isb.Offset {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferPartitionName]
}

// Update will update the existing item or add if not present for the given toBuffer partition name.
func (im *idleManager) Update(fromBufferPartitionIndex int32, toBufferPartitionName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// clear means the fromBufferPartition forwarder is inactive as the default value is 0
	im.forwarderActiveToPartition[toBufferPartitionName] = clearBit(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex))
	im.wmbOffset[toBufferPartitionName] = newOffset
}

// MarkActive marks active for the given toBuffer partition name.
func (im *idleManager) MarkActive(fromBufferPartitionIndex int32, toBufferPartitionName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// set to 1 means mark the fromBufferPartition forwarder as active
	im.forwarderActiveToPartition[toBufferPartitionName] = setBit(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex))
	im.wmbOffset[toBufferPartitionName] = nil
}

// MarkIdle marks idle for the given toBuffer partition name if it's not idle.
func (im *idleManager) MarkIdle(fromBufferPartitionIndex int32, toBufferPartitionName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	if !isBitSet(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex)) {
		// the bit value at position fromBufferPartitionIndex is 0, meaning it's already idle, can skip
		return
	}
	// set active to idle
	im.forwarderActiveToPartition[toBufferPartitionName] = clearBit(im.forwarderActiveToPartition[toBufferPartitionName], uint(fromBufferPartitionIndex))
}

// setBit sets the bit at pos in the integer n.
func setBit(n uint64, pos uint) uint64 {
	n |= uint64(1) << pos
	return n
}

// clearBit clears the bit at pos in n.
func clearBit(n uint64, pos uint) uint64 {
	n &^= uint64(1) << pos
	return n
}

// isBitSet checks if the bit at pos in n is set to 1.
func isBitSet(n uint64, pos uint) bool {
	conditionCheck := uint64(1) << pos
	return (n & conditionCheck) != 0
}

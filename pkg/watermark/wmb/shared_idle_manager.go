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
	// forwarderIdlePartition is a map[fromPartitionName]map[toPartitionName]bool to record the activeToPartitions
	// for the given forwarder.
	forwarderActiveToPartition map[string]map[string]bool
	lock                       sync.RWMutex
}

// NewSharedIdleManager returns an sharedIdleManager object as the IdleManager interface type to track the watermark idle status.
func NewSharedIdleManager(fromBufferPartitions []string, numOfToPartitions int) (IdleManager, error) {
	var forwarderIdleToPartition = make(map[string]map[string]bool, len(fromBufferPartitions))
	if fromBufferPartitions == nil {
		return nil, fmt.Errorf("missing fromBufferPartitions to create a new shared idle manager")
	} else {
		for _, fromPartition := range fromBufferPartitions {
			// default value is false for all to partitions
			// meaning all of them are considered as inactive when the pod starts
			forwarderIdleToPartition[fromPartition] = make(map[string]bool)
		}
	}
	return &sharedIdleManager{
		wmbOffset:                  make(map[string]isb.Offset, numOfToPartitions),
		forwarderActiveToPartition: forwarderIdleToPartition,
	}, nil
}

// NeedToSendCtrlMsg returns true if the given partition hasn't got any control message and needs to create a new control message
func (im *sharedIdleManager) NeedToSendCtrlMsg(toBufferPartitionName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()
	for _, activePartitions := range im.forwarderActiveToPartition {
		if activePartitions[toBufferPartitionName] {
			// at least one (forwarder, toBufferPartition) is active
			// no need to send ctrl msg
			return false
		}
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
func (im *sharedIdleManager) Update(fromBufferPartitionName string, toBufferPartitionName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.forwarderActiveToPartition[fromBufferPartitionName][toBufferPartitionName] = false
	im.wmbOffset[toBufferPartitionName] = newOffset
}

// Reset will clear the item for the given toBuffer partition name.
func (im *sharedIdleManager) Reset(fromBufferPartitionName string, toBufferPartitionName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// set to true means mark the fromBufferPartition forwarder as active
	im.forwarderActiveToPartition[fromBufferPartitionName][toBufferPartitionName] = true
	im.wmbOffset[toBufferPartitionName] = nil
}

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

import "github.com/numaproj/numaflow/pkg/isb"

// IdleManager decides when to send a control message and also keeps track of idle watermark's offset.
type IdleManager interface {
	// NeedToSendCtrlMsg validates whether to send a control message for the given partition or not.
	NeedToSendCtrlMsg(toBufferPartitionName string) bool
	// Get gets the isb.Offset from the given partition.
	Get(toBufferPartitionName string) isb.Offset
	// Update updates the isb.Offset for the given partition using the new offset if the partition exists, otherwise
	// create a new entry.
	Update(toBufferPartitionName string, newOffset isb.Offset)
	// Reset resets the idle status for the given partition.
	Reset(toBufferPartitionName string)
}

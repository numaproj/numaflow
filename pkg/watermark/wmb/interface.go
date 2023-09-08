package wmb

import "github.com/numaproj/numaflow/pkg/isb"

// IdleManagement decides when to send a control message and also keeps track of idle watermark's offset.
type IdleManagement interface {
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

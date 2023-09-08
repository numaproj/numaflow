package wmb

import "github.com/numaproj/numaflow/pkg/isb"

// IdleManagerInterface decides when to send a control message and also keeps track of idle watermark's offset.
type IdleManagerInterface interface {
	// NeedToSendCtrlMsg validates whether to send a control message for the given partition or not
	NeedToSendCtrlMsg(toBufferPartitionName string) bool
	Get(toBufferPartitionName string) isb.Offset
	Update(toBufferPartitionName string, newOffset isb.Offset)
	Reset(toBufferPartitionName string)
}

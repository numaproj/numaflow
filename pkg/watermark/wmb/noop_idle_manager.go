package wmb

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

type noOpIdleManager struct {
}

// NewNoOpIdleManager returns an no op idleManager object
func NewNoOpIdleManager() IdleManager {
	return &noOpIdleManager{}
}

func (n *noOpIdleManager) NeedToSendCtrlMsg(string) bool {
	// no op idle manager won't write any ctrl message
	// so always return false
	return false
}

func (n *noOpIdleManager) Get(string) isb.Offset {
	return isb.SimpleIntOffset(func() int64 { return int64(-1) })
}

func (n *noOpIdleManager) Update(string, isb.Offset) {
}

func (n *noOpIdleManager) Reset(string) {
}

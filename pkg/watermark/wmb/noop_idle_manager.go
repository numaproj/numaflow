package wmb

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

type NoOpIdleManager struct {
}

// NewNoOpIdleManager returns an no op IdleManager object
func NewNoOpIdleManager() *NoOpIdleManager {
	return &NoOpIdleManager{}
}

func (n *NoOpIdleManager) Validate(string) bool {
	// no op idle manager won't write any ctrl message
	// so always return false
	return false
}

func (n *NoOpIdleManager) Get(string) isb.Offset {
	return isb.SimpleIntOffset(func() int64 { return int64(-1) })
}

func (n *NoOpIdleManager) Update(string, isb.Offset) {
}

func (n *NoOpIdleManager) Reset(string) {
}

package wmb

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

type NoopIdleManager struct {
}

// NewNoopIdleManager returns an no op IdleManager object
func NewNoopIdleManager() *NoopIdleManager {
	return &NoopIdleManager{}
}

func (n NoopIdleManager) Validate(string) bool {
	// no op idle manager won't write any ctrl message
	// so always return false
	return false
}

func (n NoopIdleManager) Get(string) isb.Offset {
	return isb.SimpleIntOffset(func() int64 { return int64(-1) })
}

func (n NoopIdleManager) Update(string, isb.Offset) {
	return
}

func (n NoopIdleManager) Reset(string) {
	return
}

package wmb

import (
	"testing"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/stretchr/testify/assert"
)

func TestNewIdleManager(t *testing.T) {
	var (
		toBufferName = "testBuffer"
		o            = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
	)
	idleManager := NewIdleManager(10)
	assert.NotNil(t, idleManager)
	assert.False(t, idleManager.Exists(toBufferName))
	idleManager.Update(toBufferName, o)
	getOffset := idleManager.Get(toBufferName)
	assert.Equal(t, o.String(), getOffset.String())
	assert.True(t, idleManager.Exists(toBufferName))
	idleManager.Reset(toBufferName)
	assert.False(t, idleManager.Exists(toBufferName))
}

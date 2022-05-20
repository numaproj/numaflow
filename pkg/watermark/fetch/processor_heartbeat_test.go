//go:build isb_jetstream

package fetch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessorHeartbeat(t *testing.T) {
	hb := NewProcessorHeartbeat()
	hb.Put("pod1", 1)
	assert.Equal(t, int64(1), hb.Get("pod1"))
	hb.Put("pod1", 5)
	assert.Equal(t, int64(5), hb.Get("pod1"))
	hb.Put("pod2", 6)
	assert.Equal(t, map[string]int64{"pod1": int64(5), "pod2": int64(6)}, hb.GetAll())
	hb.Delete("pod1")
	assert.Equal(t, map[string]int64{"pod2": int64(6)}, hb.GetAll())
}

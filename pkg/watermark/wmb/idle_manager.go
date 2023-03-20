package wmb

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

// IdleManager manages the idle watermark status
type IdleManager struct {
	// wmbOffset is a toBufferName to the write offset of the idle watermark map.
	wmbOffset map[string]isb.Offset
	lock      sync.RWMutex
}

// NewIdleManager returns an IdleManager object to track the watermark idle status.
func NewIdleManager(length int) *IdleManager {
	return &IdleManager{
		wmbOffset: make(map[string]isb.Offset, length),
	}
}

// Exists returns true if the given toBuffer name exists in the IdleManager map.
func (im *IdleManager) Exists(toBufferName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferName] != nil
}

// Get gets the offset for the given toBufferName
func (im *IdleManager) Get(toBufferName string) isb.Offset {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.wmbOffset[toBufferName]
}

// Update will update the existing item or add if not present for the given toBuffer name.
func (im *IdleManager) Update(toBufferName string, newOffset isb.Offset) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferName] = newOffset
}

// Reset will clear the item for the given toBuffer name.
func (im *IdleManager) Reset(toBufferName string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.wmbOffset[toBufferName] = nil
}

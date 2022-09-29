package keyed

import (
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
	"sync"
)

// KeyedWindow maintains association between keys and a window.
// In a keyed stream, we need to close all the partitions when the watermark is past the window.
type KeyedWindow struct {
	*window.IntervalWindow
	Keys []string
	lock sync.RWMutex
}

// NewKeyedWindow creates a new keyed window
func NewKeyedWindow(window *window.IntervalWindow) *KeyedWindow {
	kw := &KeyedWindow{
		IntervalWindow: window,
		Keys:           make([]string, 0),
		lock:           sync.RWMutex{},
	}
	return kw
}

// AddKey adds a key to an existing window
func (kw *KeyedWindow) AddKey(key string) {
	kw.lock.Lock()
	defer kw.lock.Unlock()
	kw.Keys = append(kw.Keys, key)
}

// Partitions returns an array of partitions for a window
func (kw *KeyedWindow) Partitions() []partition.ID {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	partitions := make([]partition.ID, len(kw.Keys))
	for i, key := range kw.Keys {
		partitions[i] = partition.ID{Start: kw.IntervalWindow.Start, End: kw.IntervalWindow.End, Key: key}
	}

	return partitions
}

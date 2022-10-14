package keyed

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// KeyedWindow maintains association between keys and a window.
// In a keyed stream, we need to close all the partitions when the watermark is past the window.
type KeyedWindow struct {
	*window.IntervalWindow
	Keys map[string]string
	lock sync.RWMutex
}

// NewKeyedWindow creates a new keyed window
func NewKeyedWindow(window *window.IntervalWindow) *KeyedWindow {
	kw := &KeyedWindow{
		IntervalWindow: window,
		Keys:           make(map[string]string),
		lock:           sync.RWMutex{},
	}
	return kw
}

// AddKey adds a key to an existing window
func (kw *KeyedWindow) AddKey(key string) {
	kw.lock.Lock()
	defer kw.lock.Unlock()
	if _, ok := kw.Keys[key]; !ok {
		kw.Keys[key] = key
	}
}

// Partitions returns an array of partitions for a window
func (kw *KeyedWindow) Partitions() []partition.ID {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	partitions := make([]partition.ID, len(kw.Keys))
	idx := 0
	for k := range kw.Keys {
		partitions[idx] = partition.ID{Start: kw.IntervalWindow.Start, End: kw.IntervalWindow.End, Key: k}
		idx++
	}

	return partitions
}

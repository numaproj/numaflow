package aligned

import (
	"fmt"
	"github.com/numaproj/numaflow/pkg/window"
	"sync"
)

// PartitionId uniquely identifies a partition
type PartitionId string

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
func (kw *KeyedWindow) Partitions() []PartitionId {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	partitions := make([]PartitionId, len(kw.Keys))
	for i, key := range kw.Keys {
		partitions[i] = Partition(kw.IntervalWindow, key)
	}

	return partitions
}

// Partition returns the partitionId for a given window and a key
func Partition(window *window.IntervalWindow, key string) PartitionId {
	return PartitionId(fmt.Sprintf("%s-%v-%v", key, window.Start.Unix(), window.End.Unix()))
}

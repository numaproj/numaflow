package keyed

import (
	"fmt"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

// PartitionID uniquely identifies a partition
type PartitionID struct {
	Start time.Time
	End   time.Time
	Key   string
}

func (p PartitionID) String() string {
	return fmt.Sprintf("%v-%v-%s", p.Start.Unix(), p.End.Unix(), p.Key)
}

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
func (kw *KeyedWindow) Partitions() []PartitionID {
	kw.lock.RLock()
	defer kw.lock.RUnlock()

	partitions := make([]PartitionID, len(kw.Keys))
	for i, key := range kw.Keys {
		partitions[i] = PartitionID{Start: kw.IntervalWindow.Start, End: kw.IntervalWindow.End, Key: key}
	}

	return partitions
}

package aligned

import (
	"container/list"
	"fmt"
	"github.com/numaproj/numaflow/pkg/window"
	"sync"
	"time"
)

// Windows maintains the state of active windows
// All the operations in Windows order the entries in the ascending order of start time.
// So the earliest window is at the front and the oldest window is at the end.
type Windows struct {
	entries *list.List
	lock    sync.Mutex
}

// PartitionId uniquely identifies a partition
type PartitionId string

// KeyedWindow maintains association between keys and a window
type KeyedWindow struct {
	*window.IntervalWindow
	Keys []string
	lock sync.Mutex
}

// NewWindows initializes the windows object.
func NewWindows() *Windows {
	return &Windows{
		entries: list.New(),
		lock:    sync.Mutex{},
	}
}

// Partitions returns an array of partitions for a window
func (kw *KeyedWindow) Partitions() []PartitionId {
	kw.lock.Lock()
	defer kw.lock.Unlock()

	partitions := make([]PartitionId, len(kw.Keys))
	for i, key := range kw.Keys {
		partitions[i] = Partition(kw.IntervalWindow, key)
	}

	return partitions
}

// CreateOrGetAlignedWindow gets and adds a new keyed window for a given interval window
func (w *Windows) CreateOrGetAlignedWindow(window *window.IntervalWindow) *KeyedWindow {
	w.lock.Lock()
	defer w.lock.Unlock()

	var existingWindow *KeyedWindow

	// first check if we already have a window
	for e := w.entries.Front(); e != nil; e = e.Next() {
		win := e.Value.(*KeyedWindow)
		if win.Start == window.Start && win.End == win.End {
			existingWindow = win
			break
		}
	}

	if existingWindow != nil {
		return existingWindow
	}

	kw := &KeyedWindow{
		IntervalWindow: window,
		Keys:           make([]string, 0),
	}

	// this could be the first window
	if w.entries.Len() == 0 {
		w.entries.PushFront(kw)
	}

	earliestWindow := w.entries.Front().Value.(*KeyedWindow)
	recentWindow := w.entries.Back().Value.(*KeyedWindow)

	// late arrival
	if earliestWindow.Start == kw.End || earliestWindow.Start.After(kw.End) {
		w.entries.PushFront(kw)
	} else if recentWindow.End == kw.Start || recentWindow.End.Before(kw.Start) {
		w.entries.PushBack(kw)
	} else {

		for e := w.entries.Front(); e != nil; e = e.Next() {
			win := e.Value.(*KeyedWindow)
			if win.Start.After(kw.End) || win.Start == kw.End {
				w.entries.InsertBefore(kw, e)
				break
			}
		}
	}
	return kw
}

// RemoveWindow returns an array of keyed windows that are before the current watermark.
// So these windows can be closed.
func (w *Windows) RemoveWindow(wm time.Time) []*KeyedWindow {
	w.lock.Lock()
	defer w.lock.Unlock()

	closedWindows := make([]*KeyedWindow, 0)
	for e := w.entries.Front(); e != nil; e = e.Next() {
		win := e.Value.(*KeyedWindow)
		if win.End.Before(wm) {
			removed := w.entries.Remove(e)
			closedWindows = append(closedWindows, removed.(*KeyedWindow))
		}
	}

	return closedWindows
}

// AddKey adds a key to an existing window
func (kw *KeyedWindow) AddKey(key string) {
	kw.lock.Lock()
	defer kw.lock.Unlock()
	kw.Keys = append(kw.Keys, key)
}

// Partition returns the partitionId for a given window and a key
func Partition(window *window.IntervalWindow, key string) PartitionId {
	return PartitionId(fmt.Sprintf("%s-%v-%v", key, window.Start.UnixMilli(), window.End.UnixMilli()))
}

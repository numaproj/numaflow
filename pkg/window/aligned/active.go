// Package aligned maintains the state of active keyed windows in a vertex.
// Keyed Window maintains the association between set of keys and an interval window.
// aligned provides functionality to Add, Get window for a given interval, remove windows whose End time is before the
// current watermark.
package aligned

import (
	"container/list"
	"github.com/numaproj/numaflow/pkg/window"
	"sync"
	"time"
)

// ActiveWindows maintains the state of active windows
// All the operations in ActiveWindows order the entries in the ascending order of start time.
// So the earliest window is at the front and the oldest window is at the end.
type ActiveWindows struct {
	entries *list.List
	lock    sync.RWMutex
}

// NewWindows initializes the windows object.
func NewWindows() *ActiveWindows {
	return &ActiveWindows{
		entries: list.New(),
		lock:    sync.RWMutex{},
	}
}

// CreateKeyedWindow adds a new keyed window for a given interval window
func (aw *ActiveWindows) CreateKeyedWindow(iw *window.IntervalWindow) *KeyedWindow {
	aw.lock.Lock()
	defer aw.lock.Unlock()

	kw := NewKeyedWindow(iw)

	// this could be the first window
	if aw.entries.Len() == 0 {
		aw.entries.PushFront(kw)
		return kw
	}

	earliestWindow := aw.entries.Front().Value.(*KeyedWindow)
	recentWindow := aw.entries.Back().Value.(*KeyedWindow)

	// late arrival
	if earliestWindow.Start == kw.End || earliestWindow.Start.After(kw.End) {
		aw.entries.PushFront(kw)
	} else if recentWindow.End == kw.Start || recentWindow.End.Before(kw.Start) {
		// early arrival
		aw.entries.PushBack(kw)
	} else {
		// a window in the middle
		for e := aw.entries.Back(); e != nil; e = e.Prev() {
			win := e.Value.(*KeyedWindow)
			if win.Start.After(kw.End) || win.Start == kw.End {
				aw.entries.InsertBefore(kw, e)
				break
			}
		}
	}
	return kw
}

// GetKeyedWindow returns an existing window for the given interval
func (aw *ActiveWindows) GetKeyedWindow(iw *window.IntervalWindow) *KeyedWindow {
	aw.lock.RLock()
	defer aw.lock.RUnlock()
	// check if we already have a window
	for e := aw.entries.Back(); e != nil; e = e.Prev() {
		win := e.Value.(*KeyedWindow)
		if win.Start == iw.Start && win.End == iw.End {
			return win
		}
	}
	return nil
}

// RemoveWindow returns an array of keyed windows that are before the current watermark.
// So these windows can be closed.
func (aw *ActiveWindows) RemoveWindow(wm time.Time) []*KeyedWindow {
	aw.lock.Lock()
	defer aw.lock.Unlock()

	closedWindows := make([]*KeyedWindow, 0)

	for e := aw.entries.Front(); e != nil; {
		win := e.Value.(*KeyedWindow)
		next := e.Next()
		// remove window only after the watermark has passed the end of the window
		if win.End.Before(wm) {
			aw.entries.Remove(e)
			closedWindows = append(closedWindows, win)
		}
		e = next
	}

	return closedWindows
}

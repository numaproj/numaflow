/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fixed

import (
	"container/list"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

// ActiveWindows maintains the state of active windows
// All the operations in ActiveWindows order the entries in the ascending order of start time.
// So the earliest window is at the front and the oldest window is at the end.
type ActiveWindows struct {
	// entries is the list of active windows that are currently being tracked.
	// windows are sorted in chronological order with the earliest window at the head of the list.
	// list.List is implemented as a doubly linked list which allows us to traverse the nodes in
	// both the directions.
	// Although the worst case time complexity is O(n), because of the time based ordering and
	// since the elements are rarely out of order, the amortized complexity works out to be closer to O(1)
	// Because most of the keys are expected to be associated with the most recent window, we always start
	// the traversal from the tail of the list for Get and Create Operations. For Remove Operations, since
	// the earlier windows are expected to be closed before the more recent ones, we start the traversal
	// from the Head.
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
func (aw *ActiveWindows) CreateKeyedWindow(iw *window.IntervalWindow) *keyed.KeyedWindow {
	aw.lock.Lock()
	defer aw.lock.Unlock()

	kw := keyed.NewKeyedWindow(iw)

	// this could be the first window
	if aw.entries.Len() == 0 {
		aw.entries.PushFront(kw)
		return kw
	}

	earliestWindow := aw.entries.Front().Value.(*keyed.KeyedWindow)
	recentWindow := aw.entries.Back().Value.(*keyed.KeyedWindow)

	// late arrival
	if !earliestWindow.Start.Before(kw.End) {
		aw.entries.PushFront(kw)
	} else if !recentWindow.End.After(kw.Start) {
		// early arrival
		aw.entries.PushBack(kw)
	} else {
		// a window in the middle
		for e := aw.entries.Back(); e != nil; e = e.Prev() {
			win := e.Value.(*keyed.KeyedWindow)
			if !win.Start.Before(kw.End) {
				aw.entries.InsertBefore(kw, e)
				break
			}
		}
	}
	return kw
}

// GetKeyedWindow returns an existing window for the given interval
func (aw *ActiveWindows) GetKeyedWindow(iw *window.IntervalWindow) *keyed.KeyedWindow {
	aw.lock.RLock()
	defer aw.lock.RUnlock()

	if aw.entries.Len() == 0 {
		return nil
	}

	// are we looking for a window that is later than the current latest?
	latest := aw.entries.Back()
	lkw := latest.Value.(*keyed.KeyedWindow)
	if !lkw.End.After(iw.Start) {
		return nil
	}

	// are we looking for a window that is earlier than the current earliest?
	earliest := aw.entries.Front()
	ekw := earliest.Value.(*keyed.KeyedWindow)
	if !ekw.Start.Before(iw.End) {
		return nil
	}

	// check if we already have a window
	for e := aw.entries.Back(); e != nil; e = e.Prev() {
		win := e.Value.(*keyed.KeyedWindow)
		if win.Start.Equal(iw.Start) && win.End.Equal(iw.End) {
			return win
		} else if win.Start.Before(iw.End) {
			// we have moved past the range that we are looking for
			// so, we can bail out early.
			break
		}
	}
	return nil
}

// RemoveWindow returns an array of keyed windows that are before the current watermark.
// So these windows can be closed.
func (aw *ActiveWindows) RemoveWindow(wm time.Time) []*keyed.KeyedWindow {
	aw.lock.Lock()
	defer aw.lock.Unlock()

	closedWindows := make([]*keyed.KeyedWindow, 0)

	for e := aw.entries.Front(); e != nil; {
		win := e.Value.(*keyed.KeyedWindow)
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

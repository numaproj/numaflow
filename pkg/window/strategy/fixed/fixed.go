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

// Package fixed implements Fixed windows. Fixed windows (sometimes called tumbling windows) are
// defined by a static window size, e.g. minutely windows or hourly windows. They are generally aligned, i.e. every
// window applies across all the data for the corresponding period of time.
// Package fixed also maintains the state of active keyed windows in a vertex.
// Keyed Window maintains the association between set of keys and an interval window.
// keyed also provides the lifecycle management of an interval window. Watermark is used to trigger the expiration of windows.
package fixed

import (
	"container/list"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

// Fixed implements Fixed window.
// Fixed maintains the state of active windows
// All the operations in Fixed (see window.Windower) order the entries in the ascending order of start time.
// So the earliest window is at the front and the oldest window is at the end.
type Fixed struct {
	// Length is the temporal length of the window.
	Length time.Duration
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

var _ window.Windower = (*Fixed)(nil)

// NewFixed returns a Fixed windower.
func NewFixed(length time.Duration) window.Windower {
	return &Fixed{
		Length:  length,
		entries: list.New(),
		lock:    sync.RWMutex{},
	}
}

// AssignWindow assigns a window for the given eventTime.
func (f *Fixed) AssignWindow(eventTime time.Time) []window.AlignedKeyedWindower {
	start := eventTime.Truncate(f.Length)
	end := start.Add(f.Length)

	// Assignment of windows should follow a Left inclusive and right exclusive
	// principle. Since we use truncate here, it is guaranteed that any element
	// on the boundary will automatically fall in to the window to the right
	// of the boundary thereby satisfying the requirement.
	return []window.AlignedKeyedWindower{
		keyed.NewKeyedWindow(start, end),
	}
}

// CreateWindow adds a window for a given interval window
func (f *Fixed) CreateWindow(kw window.AlignedKeyedWindower) window.AlignedKeyedWindower {
	f.lock.Lock()
	defer f.lock.Unlock()

	// this could be the first window
	if f.entries.Len() == 0 {
		f.entries.PushFront(kw)
		return kw
	}

	earliestWindow := f.entries.Front().Value.(*keyed.AlignedKeyedWindow)
	recentWindow := f.entries.Back().Value.(*keyed.AlignedKeyedWindow)

	// late arrival
	if !earliestWindow.StartTime().Before(kw.EndTime()) {
		f.entries.PushFront(kw)
	} else if !recentWindow.EndTime().After(kw.StartTime()) {
		// early arrival
		f.entries.PushBack(kw)
	} else {
		// a window in the middle
		for e := f.entries.Back(); e != nil; e = e.Prev() {
			win := e.Value.(*keyed.AlignedKeyedWindow)
			if !win.StartTime().Before(kw.EndTime()) {
				f.entries.InsertBefore(kw, e)
				break
			}
		}
	}
	return kw
}

// GetWindow returns an existing window for the given interval
func (f *Fixed) GetWindow(kw window.AlignedKeyedWindower) window.AlignedKeyedWindower {
	f.lock.RLock()
	defer f.lock.RUnlock()

	if f.entries.Len() == 0 {
		return nil
	}

	// are we looking for a window that is later than the current latest?
	latest := f.entries.Back()
	lkw := latest.Value.(*keyed.AlignedKeyedWindow)
	if !lkw.EndTime().After(kw.StartTime()) {
		return nil
	}

	// are we looking for a window that is earlier than the current earliest?
	earliest := f.entries.Front()
	ekw := earliest.Value.(*keyed.AlignedKeyedWindow)
	if !ekw.StartTime().Before(kw.EndTime()) {
		return nil
	}

	// check if we already have a window
	for e := f.entries.Back(); e != nil; e = e.Prev() {
		win := e.Value.(*keyed.AlignedKeyedWindow)
		if win.StartTime().Equal(kw.StartTime()) && win.EndTime().Equal(kw.EndTime()) {
			return win
		} else if win.StartTime().Before(kw.EndTime()) {
			// we have moved past the range that we are looking for
			// so, we can bail out early.
			break
		}
	}
	return nil
}

// RemoveWindows returns an array of keyed windows that are before the current watermark.
// So these windows can be closed.
func (f *Fixed) RemoveWindows(wm time.Time) []window.AlignedKeyedWindower {
	f.lock.Lock()
	defer f.lock.Unlock()

	closedWindows := make([]window.AlignedKeyedWindower, 0)

	for e := f.entries.Front(); e != nil; {
		win := e.Value.(*keyed.AlignedKeyedWindow)
		next := e.Next()
		// remove window only after the watermark has passed the end of the window
		if win.EndTime().Before(wm) {
			f.entries.Remove(e)
			closedWindows = append(closedWindows, win)
		}
		e = next
	}

	return closedWindows
}

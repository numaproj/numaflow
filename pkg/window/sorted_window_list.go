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

package window

import (
	"container/list"
	"sync"
	"time"
)

// SortedWindowList is a thread safe list implementation, which is sorted by window start time
// from lowest to highest
type SortedWindowList[W AlignedWindower] struct {
	windows *list.List
	lock    *sync.RWMutex
}

// NewSortedWindowList implements a window list ordered by the start time. The Front/Head of the list will always have the smallest
// element while the End/Tail will have the largest element (start time).
func NewSortedWindowList[W AlignedWindower]() *SortedWindowList[W] {
	return &SortedWindowList[W]{
		windows: list.New(),
		lock:    new(sync.RWMutex),
	}
}

// InsertFront tries to insert the window to the Front of the list as long as the window is smaller than the current
// Front/Head.
func (s *SortedWindowList[W]) InsertFront(kw W) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.windows.Len() == 0 {
		s.windows.PushFront(kw)
		return
	}
	if !s.windows.Front().Value.(W).StartTime().After(kw.StartTime()) {
		return
	}
	s.windows.PushFront(kw)
}

// InsertBack tries to insert the window to the Back of the list as long as the window is larger than the current
// Back/Tail.
func (s *SortedWindowList[W]) InsertBack(kw W) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.windows.Len() == 0 {
		s.windows.PushBack(kw)
		return
	}
	if !s.windows.Back().Value.(W).StartTime().Before(kw.StartTime()) {
		return
	}
	s.windows.PushBack(kw)
}

// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window.
func (s *SortedWindowList[W]) InsertIfNotPresent(kw W) (aw W, isPresent bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// this could be the first window
	if s.windows.Len() == 0 {
		s.windows.PushFront(kw)
		return kw, false
	}

	earliestWindow := s.windows.Front().Value.(W)
	recentWindow := s.windows.Back().Value.(W)

	// if there is only one window
	if earliestWindow.StartTime().Equal(kw.StartTime()) && earliestWindow.EndTime().Equal(kw.EndTime()) {
		aw = earliestWindow
		isPresent = true
	} else if earliestWindow.StartTime().After(kw.StartTime()) {
		// late arrival
		s.windows.PushFront(kw)
		aw = kw
	} else if recentWindow.StartTime().Before(kw.StartTime()) {
		// early arrival
		s.windows.PushBack(kw)
		aw = kw
	} else {
		// a window in the middle
		for e := s.windows.Back(); e.Prev() != nil; e = e.Prev() {
			win := e.Value.(W)
			prevWin := e.Prev().Value.(W)
			if win.StartTime().Equal(kw.StartTime()) && win.EndTime().Equal(kw.EndTime()) {
				aw = win
				isPresent = true
				break
			}
			if win.StartTime().After(kw.StartTime()) && prevWin.StartTime().Before(kw.StartTime()) {
				s.windows.InsertBefore(kw, e)
				aw = kw
				break
			}
		}
	}
	return
}

// DeleteWindow deletes a window from the list.
func (s *SortedWindowList[W]) DeleteWindow(kw W) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for e := s.windows.Front(); e != nil; {
		win := e.Value.(W)
		next := e.Next()

		if win.StartTime().Equal(kw.StartTime()) && win.EndTime().Equal(kw.EndTime()) {
			s.windows.Remove(e)
		}
		e = next
	}
}

// RemoveWindows removes a set of windows smaller than or equal to the given time.
func (s *SortedWindowList[W]) RemoveWindows(tm time.Time) []W {

	s.lock.Lock()
	defer s.lock.Unlock()

	closedWindows := make([]W, 0)

	if s.windows.Len() == 0 {
		return closedWindows
	}
	// examine the earliest window
	earliestWindow := s.windows.Front().Value.(W)
	if earliestWindow.EndTime().After(tm) {
		// no windows to close since the watermark is behind the earliest window
		return closedWindows
	}

	// close the window if the window end time is equal to the watermark
	// because window is right exclusive
	for e := s.windows.Front(); e != nil; {
		win := e.Value.(W)
		next := e.Next()

		// break, if we find a window with end time > watermark
		if win.EndTime().After(tm) {
			break
		}

		s.windows.Remove(e)
		closedWindows = append(closedWindows, win)
		e = next
	}

	return closedWindows
}

// Len returns the length of the window.
func (s *SortedWindowList[W]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.windows.Len()
}

// Front returns the smallest element from the list.
func (s *SortedWindowList[W]) Front() W {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// FIXME: add len == 0 check
	return s.windows.Front().Value.(W)
}

// Back returns the largest element from the list.
func (s *SortedWindowList[W]) Back() W {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// FIXME: add len == 0 check
	return s.windows.Back().Value.(W)
}

// Items returns the entire window list.
func (s *SortedWindowList[W]) Items() []W {
	s.lock.RLock()
	defer s.lock.RUnlock()
	items := make([]W, 0)

	for e := s.windows.Front(); e != nil; {
		items = append(items, e.Value.(W))
		e = e.Next()
	}

	return items
}

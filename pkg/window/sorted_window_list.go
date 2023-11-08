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
	"sort"
	"sync"
	"time"
)

// SortedWindowList is a thread safe list implementation, which is sorted by window start time
// from lowest to highest
type SortedWindowList[W TimedWindow] struct {
	windows []W
	lock    *sync.RWMutex
}

// NewSortedWindowList implements a window list ordered by the start time. The Front/Head of the list will always have the smallest
// element while the End/Tail will have the largest element (start time).
func NewSortedWindowList[W TimedWindow]() *SortedWindowList[W] {
	return &SortedWindowList[W]{
		windows: make([]W, 0),
		lock:    &sync.RWMutex{},
	}
}

// InsertFront inserts a window to the front of the list.
func (s *SortedWindowList[W]) InsertFront(window W) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.windows) != 0 && (s.windows[0].StartTime().Before(window.StartTime()) ||
		s.windows[0].Partition().String() == window.Partition().String()) {
		return false
	}

	s.windows = append([]W{window}, s.windows...)
	return true
}

// InsertBack inserts a window to the end of the list.
func (s *SortedWindowList[W]) InsertBack(window W) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.windows) != 0 && (s.windows[len(s.windows)-1].StartTime().After(window.StartTime()) ||
		s.windows[len(s.windows)-1].Partition().String() == window.Partition().String()) {
		return false
	}

	s.windows = append(s.windows, window)
	return true
}

// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window.
func (s *SortedWindowList[W]) InsertIfNotPresent(window W) (W, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(window.StartTime())
	})

	updatedIndex := index

	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() {
			return s.windows[i], true
		}

		if s.windows[i].StartTime().After(window.StartTime()) {
			updatedIndex = i
			break
		}
	}

	s.windows = append(s.windows, window)
	copy(s.windows[updatedIndex+1:], s.windows[updatedIndex:])
	s.windows[updatedIndex] = window

	return window, false
}

// Delete deletes a window from the list.
func (s *SortedWindowList[W]) Delete(window W) (deleted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted = false

	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(window.StartTime())
	})

	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() {
			s.windows = append(s.windows[:i], s.windows[i+1:]...)
			deleted = true
			break
		}

		if s.windows[i].StartTime().After(window.StartTime()) {
			break
		}
	}
	return deleted
}

// RemoveWindows removes a set of windows smaller than or equal to the given time.
func (s *SortedWindowList[W]) RemoveWindows(t time.Time) []W {
	s.lock.Lock()
	defer s.lock.Unlock()

	index := sort.Search(len(s.windows), func(i int) bool {
		return s.windows[i].EndTime().After(t)
	})

	removed := make([]W, index)
	copy(removed, s.windows[:index])

	s.windows = s.windows[index:]

	return removed
}

// Len returns the length of the window.
func (s *SortedWindowList[W]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.windows)
}

// Front returns the smallest element from the list.
func (s *SortedWindowList[W]) Front() W {
	var front W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return front
	}
	return s.windows[0]
}

// Back returns the largest element from the list.
func (s *SortedWindowList[W]) Back() W {
	var back W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return back
	}
	return s.windows[len(s.windows)-1]
}

// Items returns the entire window list.
func (s *SortedWindowList[W]) Items() []W {
	s.lock.RLock()
	defer s.lock.RUnlock()

	items := make([]W, len(s.windows))
	copy(items, s.windows)

	return items
}

func (s *SortedWindowList[W]) FindWindowForTime(t time.Time) (W, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(t)
	})

	for i := index; i < len(s.windows); i++ {
		if !s.windows[i].StartTime().Before(t) && s.windows[i].EndTime().After(t) {
			return s.windows[i], true
		}

		if s.windows[i].StartTime().After(t) {
			break
		}
	}

	var empty W
	return empty, false
}

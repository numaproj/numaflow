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

// SortedWindowListByStartTime is a thread safe list implementation, which is sorted by window start time
// from lowest to highest
type SortedWindowListByStartTime[W TimedWindow] struct {
	windows []W
	lock    *sync.RWMutex
}

// NewSortedWindowListByStartTime implements a window list ordered by the start time. The Front/Head of the list will always have the smallest
// element while the End/Tail will have the largest element (start time).
func NewSortedWindowListByStartTime[W TimedWindow]() *SortedWindowListByStartTime[W] {
	return &SortedWindowListByStartTime[W]{
		windows: make([]W, 0),
		lock:    &sync.RWMutex{},
	}
}

// InsertFront inserts a window to the front of the list.
func (s *SortedWindowListByStartTime[W]) InsertFront(window W) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the window can be inserted at the front
	if len(s.windows) != 0 && (s.windows[0].EndTime().Before(window.EndTime())) {
		return false
	}

	// Insert the window at the front
	s.windows = append([]W{window}, s.windows...)
	return true
}

// InsertBack inserts a window to the end of the list.
func (s *SortedWindowListByStartTime[W]) InsertBack(window W) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the window can be inserted at the back
	if len(s.windows) != 0 && (s.windows[len(s.windows)-1].EndTime().After(window.EndTime())) {
		return false
	}

	// Insert the window at the back
	s.windows = append(s.windows, window)
	return true
}

// Insert inserts a window to the list.
func (s *SortedWindowListByStartTime[W]) Insert(window W) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index where the window should be inserted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(window.StartTime())
	})

	// Insert the window at the end of the list, since it is the largest
	if index == len(s.windows) {
		s.windows = append(s.windows, window)
		return
	}

	// Insert the window at the middle position
	s.windows = append(s.windows[:index+1], s.windows[index:]...)
	s.windows[index] = window
}

// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window.
func (s *SortedWindowListByStartTime[W]) InsertIfNotPresent(window W) (W, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index where the window should be inserted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(window.StartTime())
	})

	updatedIndex := index

	// Check if the window is already present in the list
	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() {
			return s.windows[i], true
		}

		if s.windows[i].StartTime().After(window.StartTime()) {
			updatedIndex = i
			break
		}
	}

	// Insert the window at the correct position
	s.windows = append(s.windows, window)
	copy(s.windows[updatedIndex+1:], s.windows[updatedIndex:])
	s.windows[updatedIndex] = window

	return window, false
}

// Delete deletes a window from the list.
func (s *SortedWindowListByStartTime[W]) Delete(window W) (deleted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted = false

	// Find the index of the window to be deleted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().Before(window.StartTime())
	})

	// Delete the window if it is present in the list
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
func (s *SortedWindowListByStartTime[W]) RemoveWindows(t time.Time) []W {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index of the first window that should not be removed
	index := sort.Search(len(s.windows), func(i int) bool {
		return s.windows[i].EndTime().After(t)
	})

	// Remove the windows
	removed := make([]W, index)
	copy(removed, s.windows[:index])

	s.windows = s.windows[index:]

	return removed
}

// Len returns the length of the window.
func (s *SortedWindowListByStartTime[W]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.windows)
}

// Front returns the smallest element from the list.
func (s *SortedWindowListByStartTime[W]) Front() W {
	var front W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return front
	}
	return s.windows[0]
}

// Back returns the largest element from the list.
func (s *SortedWindowListByStartTime[W]) Back() W {
	var back W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return back
	}
	return s.windows[len(s.windows)-1]
}

// Items returns the entire window list.
func (s *SortedWindowListByStartTime[W]) Items() []W {
	s.lock.RLock()
	defer s.lock.RUnlock()

	items := make([]W, len(s.windows))
	copy(items, s.windows)

	return items
}

// FindWindowForTime finds a window for a given time.
func (s *SortedWindowListByStartTime[W]) FindWindowForTime(t time.Time) (W, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// find a window that starts before the given time
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].StartTime().After(t)
	})

	println("index - ", index)
	println("len - ", len(s.windows))

	for i := index; i < len(s.windows); i++ {
		if !s.windows[i].StartTime().After(t) && s.windows[i].EndTime().After(t) {
			println("found")
			return s.windows[i], true
		}

		if s.windows[i].StartTime().After(t) {
			break
		}
	}

	var empty W
	return empty, false
}

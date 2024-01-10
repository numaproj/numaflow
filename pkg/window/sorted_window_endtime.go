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

// SortedWindowListByEndTime is a thread safe list implementation, which is sorted by window end time
// from lowest to highest.
// All search operations are done using binary-search because it is an already sorted list.
type SortedWindowListByEndTime struct {
	windows []TimedWindow
	lock    *sync.RWMutex
}

// NewSortedWindowListByEndTime implements a window list ordered by the end time. The Front/Head of the list will always have the smallest
// element while the End/Tail will have the largest element (end time).
func NewSortedWindowListByEndTime() *SortedWindowListByEndTime {
	return &SortedWindowListByEndTime{
		windows: make([]TimedWindow, 0),
		lock:    &sync.RWMutex{},
	}
}

// InsertBack inserts a window to the back of the list.
func (s *SortedWindowListByEndTime) InsertBack(window TimedWindow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the window can be inserted at the back
	if l := len(s.windows); l != 0 && (s.windows[l-1].EndTime().After(window.EndTime())) {
		return
	}

	// Insert the window at the back
	s.windows = append(s.windows, window)
}

// InsertFront inserts a window to the front of the list.
func (s *SortedWindowListByEndTime) InsertFront(window TimedWindow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the window can be inserted at the front
	if len(s.windows) != 0 && (s.windows[0].EndTime().Before(window.EndTime())) {
		return
	}

	// Insert the window at the front
	s.windows = append([]TimedWindow{window}, s.windows...)
}

// Insert inserts a window to the list.
func (s *SortedWindowListByEndTime) Insert(window TimedWindow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index where the window should be inserted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].EndTime().Before(window.EndTime())
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
func (s *SortedWindowListByEndTime) InsertIfNotPresent(window TimedWindow) (TimedWindow, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index where the window should be inserted
	index := sort.Search(len(s.windows), func(i int) bool {
		var x = !s.windows[i].EndTime().Before(window.EndTime())
		return x
	})

	updatedIndex := index

	// Check if the window is already present in the list
	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() &&
			compareKeys(s.windows[i].Keys(), window.Keys()) {
			return s.windows[i], true
		}

		if s.windows[i].EndTime().After(window.EndTime()) {
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
func (s *SortedWindowListByEndTime) Delete(window TimedWindow) (deleted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted = false

	// Find the index of the window to be deleted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].EndTime().Before(window.EndTime())
	})

	// Delete the window if it is present in the list
	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() &&
			compareKeys(s.windows[i].Keys(), window.Keys()) {
			s.windows = append(s.windows[:i], s.windows[i+1:]...)
			deleted = true
			break
		}

		if s.windows[i].EndTime().After(window.EndTime()) {
			break
		}
	}
	return deleted
}

// RemoveWindows removes a set of windows whose end time is smaller than or equal to the given time.
// It returns the removed windows.
func (s *SortedWindowListByEndTime) RemoveWindows(t time.Time) []TimedWindow {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Find the index of the first window that should not be removed
	index := sort.Search(len(s.windows), func(i int) bool {
		return s.windows[i].EndTime().After(t)
	})

	// Remove the windows
	removed := make([]TimedWindow, index)
	copy(removed, s.windows[:index])

	s.windows = s.windows[index:]

	return removed
}

// Len returns the length of the window.
func (s *SortedWindowListByEndTime) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.windows)
}

// Front returns the smallest element from the list.
func (s *SortedWindowListByEndTime) Front() TimedWindow {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return nil
	}
	return s.windows[0]
}

// Back returns the largest element from the list.
func (s *SortedWindowListByEndTime) Back() TimedWindow {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return nil
	}
	return s.windows[len(s.windows)-1]
}

// Items returns the entire window list.
func (s *SortedWindowListByEndTime) Items() []TimedWindow {
	s.lock.RLock()
	defer s.lock.RUnlock()

	items := make([]TimedWindow, len(s.windows))
	copy(items, s.windows)

	return items
}

// FindWindowForTime finds a window for a given time. If there are multiple windows for the given time,
// it returns the first window.
func (s *SortedWindowListByEndTime) FindWindowForTime(t time.Time) (TimedWindow, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].EndTime().Before(t)
	})

	for i := index; i < len(s.windows); i++ {
		if !s.windows[i].StartTime().After(t) && s.windows[i].EndTime().After(t) {
			return s.windows[i], true
		}

		if s.windows[i].EndTime().After(t) {
			break
		}
	}

	return nil, false
}

// WindowToBeMerged finds a window to be merged with the given window.
// It returns the window to be merged and a boolean indicating if a window was found.
func (s *SortedWindowListByEndTime) WindowToBeMerged(window TimedWindow) (TimedWindow, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// find the first index of window whose end time is greater than or equal to the given window
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].EndTime().Before(window.EndTime())
	})

	// since it's only sorted by end time, we have to check for all the windows with end time greater than or equal to the given window
	// for example if the windows are (60, 70), (50, 90) and (30, 100), if we are given a window (35,45) we should return (30,100)
	// if we just check the window at index 0, we will return (60,70) which is incorrect
	for i := index; i < len(s.windows); i++ {
		if window.EndTime().After(s.windows[i].StartTime()) && window.Partition().Slot == s.windows[i].Partition().Slot &&
			compareKeys(window.Keys(), s.windows[i].Keys()) {
			return s.windows[i], true
		}
	}

	// we should also check if it can be merged with the window before the given window
	if index-1 >= 0 && s.windows[index-1].EndTime().After(window.StartTime()) && window.Partition().Slot == s.windows[index-1].Partition().Slot &&
		compareKeys(window.Keys(), s.windows[index-1].Keys()) {
		return s.windows[index-1], true
	}

	return nil, false
}

// compareKeys returns true if the keys are equal.
// The order of the keys matters, e.g. ["a", "b"] != ["b", "a"].
// A concrete example can be that we are tracking API calls among applications, each call can be represented as keys [client_id, service_id].
// In this case, ["app_1", "app_2"] != ["app_2", "app_1"] because app_1 calling app_2 is different from app_2 calling app_1.
func compareKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, key := range a {
		if key != b[i] {
			return false
		}
	}

	return true
}

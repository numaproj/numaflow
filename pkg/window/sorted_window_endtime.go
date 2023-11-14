package window

import (
	"sort"
	"sync"
	"time"
)

// SortedWindowListByEndTime is a thread safe list implementation, which is sorted by window end time
// from lowest to highest
type SortedWindowListByEndTime[W TimedWindow] struct {
	windows []W
	lock    *sync.RWMutex
}

// NewSortedWindowListByEndTime implements a window list ordered by the end time. The Front/Head of the list will always have the smallest
// element while the End/Tail will have the largest element (end time).
func NewSortedWindowListByEndTime[W TimedWindow]() *SortedWindowListByEndTime[W] {
	return &SortedWindowListByEndTime[W]{
		windows: make([]W, 0),
		lock:    &sync.RWMutex{},
	}
}

// InsertBack inserts a window to the back of the list.
func (s *SortedWindowListByEndTime[W]) InsertBack(window W) bool {
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

// InsertFront inserts a window to the front of the list.
func (s *SortedWindowListByEndTime[W]) InsertFront(window W) bool {
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

// Insert inserts a window to the list.
func (s *SortedWindowListByEndTime[W]) Insert(window W) {
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
func (s *SortedWindowListByEndTime[W]) InsertIfNotPresent(window W) (W, bool) {
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
		if s.windows[i].Partition().String() == window.Partition().String() {
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
func (s *SortedWindowListByEndTime[W]) Delete(window W) (deleted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted = false

	// Find the index of the window to be deleted
	index := sort.Search(len(s.windows), func(i int) bool {
		return !s.windows[i].EndTime().Before(window.EndTime())
	})

	// Delete the window if it is present in the list
	for i := index; i < len(s.windows); i++ {
		if s.windows[i].Partition().String() == window.Partition().String() {
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

// RemoveWindows removes a set of windows smaller than or equal to the given time.
func (s *SortedWindowListByEndTime[W]) RemoveWindows(t time.Time) []W {
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
func (s *SortedWindowListByEndTime[W]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.windows)
}

// Front returns the smallest element from the list.
func (s *SortedWindowListByEndTime[W]) Front() W {
	var front W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return front
	}
	return s.windows[0]
}

// Back returns the largest element from the list.
func (s *SortedWindowListByEndTime[W]) Back() W {
	var back W
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.windows) == 0 {
		return back
	}
	return s.windows[len(s.windows)-1]
}

// Items returns the entire window list.
func (s *SortedWindowListByEndTime[W]) Items() []W {
	s.lock.RLock()
	defer s.lock.RUnlock()

	items := make([]W, len(s.windows))
	copy(items, s.windows)

	return items
}

// FindWindowForTime finds a window for a given time.
func (s *SortedWindowListByEndTime[W]) FindWindowForTime(t time.Time) (W, bool) {
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

	var empty W
	return empty, false
}

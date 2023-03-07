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

// Package sliding implements Sliding windows. Sliding windows are defined by a static window size
// e.g. minutely windows or hourly windows and a fixed "slide". This is the duration by which the boundaries
// of the windows move once every <slide> duration.
// Package sliding also maintains the state of active windows.
// Watermark is used to trigger the expiration of windows.
package sliding

import (
	"container/list"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

// Sliding implements sliding windows
type Sliding struct {
	// Length is the duration of the window
	Length time.Duration
	// offset between successive windows.
	// successive windows are phased out by this duration.
	Slide time.Duration
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

var _ window.Windower = (*Sliding)(nil)

// NewSliding returns a Sliding windower
func NewSliding(length time.Duration, slide time.Duration) *Sliding {
	return &Sliding{
		Length:  length,
		Slide:   slide,
		entries: list.New(),
		lock:    sync.RWMutex{},
	}
}

// AssignWindow returns a set of windows that contain the element based on event time
func (s *Sliding) AssignWindow(eventTime time.Time) []window.AlignedKeyedWindower {
	windows := make([]window.AlignedKeyedWindower, 0)

	// use the highest integer multiple of slide length which is less than the eventTime
	// as the start time for the window. For example if the eventTime is 810 and slide
	// length is 70, use 770 as the startTime of the window. In that way we can be guarantee
	// consistency while assigning the messages to the windows.
	startTime := time.UnixMilli((eventTime.UnixMilli() / s.Slide.Milliseconds()) * s.Slide.Milliseconds())
	endTime := startTime.Add(s.Length)

	// startTime and endTime will be the largest timestamp window for the given eventTime,
	// using that we can create other windows by subtracting the slide length

	// since there is overlap at the boundaries
	// we attribute the element to the window to the right (higher)
	// of the boundary
	// left exclusive and right inclusive
	// so given windows 500-600 and 600-700 and the event time is 600
	// we will add the element to 600-700 window and not to the 500-600 window.
	for !startTime.After(eventTime) && endTime.After(eventTime) {
		windows = append(windows, keyed.NewKeyedWindow(startTime, endTime))
		startTime = startTime.Add(-s.Slide)
		endTime = endTime.Add(-s.Slide)
	}

	return windows

}

// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window
func (s *Sliding) InsertIfNotPresent(kw window.AlignedKeyedWindower) (aw window.AlignedKeyedWindower, isPresent bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// this could be the first window
	if s.entries.Len() == 0 {
		s.entries.PushFront(kw)
		return kw, false
	}

	earliestWindow := s.entries.Front().Value.(*keyed.AlignedKeyedWindow)
	recentWindow := s.entries.Back().Value.(*keyed.AlignedKeyedWindow)

	// if there is only one window
	if earliestWindow.StartTime().Equal(kw.StartTime()) && earliestWindow.EndTime().Equal(kw.EndTime()) {
		aw = earliestWindow
		isPresent = true
	} else if earliestWindow.StartTime().After(kw.StartTime()) {
		// late arrival
		s.entries.PushFront(kw)
		aw = kw
	} else if recentWindow.StartTime().Before(kw.StartTime()) {
		// early arrival
		s.entries.PushBack(kw)
		aw = kw
	} else {
		// a window in the middle
		for e := s.entries.Back(); e.Prev() != nil; e = e.Prev() {
			win := e.Value.(*keyed.AlignedKeyedWindow)
			prevWin := e.Prev().Value.(*keyed.AlignedKeyedWindow)
			if win.StartTime().Equal(kw.StartTime()) && win.EndTime().Equal(kw.EndTime()) {
				aw = win
				isPresent = true
				break
			}
			if win.StartTime().After(kw.StartTime()) && prevWin.StartTime().Before(kw.StartTime()) {
				s.entries.InsertBefore(kw, e)
				aw = kw
				break
			}
		}
	}
	return
}

func (s *Sliding) RemoveWindows(wm time.Time) []window.AlignedKeyedWindower {
	s.lock.Lock()
	defer s.lock.Unlock()

	closedWindows := make([]window.AlignedKeyedWindower, 0)

	// examine the earliest window
	earliestWindow := s.entries.Front().Value.(*keyed.AlignedKeyedWindow)
	if earliestWindow.EndTime().After(wm) {
		// no windows to close since the watermark is behind the earliest window
		return closedWindows
	}

	// close the window if the window end time is equal to the watermark
	// because window is right exclusive
	for e := s.entries.Front(); e != nil; {
		win := e.Value.(*keyed.AlignedKeyedWindow)
		next := e.Next()

		// break, if we find a window with end time > watermark
		if win.EndTime().After(wm) {
			break
		}

		s.entries.Remove(e)
		closedWindows = append(closedWindows, win)
		e = next
	}

	return closedWindows
}

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
	// start time of the window in to which this element certainly belongs.
	startTime := eventTime.Truncate(s.Length)
	// end time of the window in to which this element certainly belongs.
	endTime := startTime.Add(s.Length)

	minEndTime := startTime.Add(s.Length % s.Slide)
	minStartTime := minEndTime.Add(-s.Length)

	maxStartTime := endTime.Add(-(s.Length % s.Slide))
	maxEndTime := maxStartTime.Add(s.Length)

	wCount := int((maxEndTime.Sub(minEndTime)) / s.Slide)
	windows := make([]window.AlignedKeyedWindower, 0)

	for i := 0; i < wCount; i++ {
		st := minStartTime.Add(time.Duration(i) * s.Slide)
		et := st.Add(s.Length)

		// since there is overlap at the boundaries
		// we attribute the element to the window to the right (higher)
		// of the boundary
		// left exclusive and right inclusive
		// so given windows 500-600 and 600-700 and the event time is 600
		// we will add the element to 600-700 window and not to the 500-600 window.
		if eventTime.Before(st) || !eventTime.Before(et) {
			continue
		}

		akw := keyed.NewKeyedWindow(st, et)

		windows = append(windows, akw)
	}

	return windows
}

// CreateWindow returns a keyed window for a given interval window
func (s *Sliding) CreateWindow(kw window.AlignedKeyedWindower) window.AlignedKeyedWindower {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.entries.Len() == 0 {
		s.entries.PushFront(kw)
		return kw
	}

	earliestWindow := s.entries.Front().Value.(*keyed.AlignedKeyedWindow)
	recentWindow := s.entries.Back().Value.(*keyed.AlignedKeyedWindow)

	// late arrival
	if kw.StartTime().Before(earliestWindow.StartTime()) {
		s.entries.PushFront(kw)
	} else if kw.EndTime().After(recentWindow.EndTime()) {
		// early arrival
		s.entries.PushBack(kw)
	} else {
		// a window in the middle
		for e := s.entries.Back(); e != nil; e = e.Prev() {
			win := e.Value.(*keyed.AlignedKeyedWindow)
			if !win.StartTime().Before(kw.StartTime()) {
				s.entries.InsertBefore(kw, e)
				break
			}
		}
	}

	return kw
}

// GetWindow returns a keyed window for a given interval window
func (s *Sliding) GetWindow(iw window.AlignedKeyedWindower) window.AlignedKeyedWindower {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.entries.Len() == 0 {
		return nil
	}

	// are we looking for a window that is later than the current latest?
	latest := s.entries.Back()
	lkw := latest.Value.(*keyed.AlignedKeyedWindow)
	if iw.EndTime().After(lkw.EndTime()) {
		return nil
	}

	// are we looking for a window that is earlier than the current earliest?
	earliest := s.entries.Front()
	ekw := earliest.Value.(*keyed.AlignedKeyedWindow)
	if iw.StartTime().Before(ekw.StartTime()) {
		return nil
	}

	// check if we already have a window
	for e := s.entries.Back(); e != nil; e = e.Prev() {
		win := e.Value.(*keyed.AlignedKeyedWindow)
		if win.StartTime().Equal(iw.StartTime()) && win.EndTime().Equal(iw.EndTime()) {
			return win
		} else if win.StartTime().Before(iw.StartTime()) {
			// we have moved past the range that we are looking for
			// so, we can bail out early.
			break
		}
	}
	return nil
}

func (s *Sliding) RemoveWindows(wm time.Time) []window.AlignedKeyedWindower {
	s.lock.Lock()
	defer s.lock.Unlock()

	closedWindows := make([]window.AlignedKeyedWindower, 0)

	for e := s.entries.Front(); e != nil; {
		win := e.Value.(*keyed.AlignedKeyedWindow)
		next := e.Next()
		// remove window only after the watermark has passed the end of the window
		if win.EndTime().Before(wm) {
			s.entries.Remove(e)
			closedWindows = append(closedWindows, win)
		}
		e = next
	}

	return closedWindows
}

package slist

import (
	"container/list"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

type SortedWindowList[W window.AlignedKeyedWindower] struct {
	windows *list.List
	lock    *sync.RWMutex
}

func New[W window.AlignedKeyedWindower]() *SortedWindowList[W] {
	return &SortedWindowList[W]{
		windows: list.New(),
		lock:    new(sync.RWMutex),
	}
}

func (s *SortedWindowList[W]) InsertFront(kw W) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.windows.PushFront(kw)
}

func (s *SortedWindowList[W]) InsertBack(kw W) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.windows.PushBack(kw)
}

// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window
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

func (s *SortedWindowList[W]) RemoveWindows(wm time.Time) []W {

	s.lock.Lock()
	defer s.lock.Unlock()

	closedWindows := make([]W, 0)

	if s.windows.Len() == 0 {
		return closedWindows
	}
	// examine the earliest window
	earliestWindow := s.windows.Front().Value.(W)
	if earliestWindow.EndTime().After(wm) {
		// no windows to close since the watermark is behind the earliest window
		return closedWindows
	}

	// close the window if the window end time is equal to the watermark
	// because window is right exclusive
	for e := s.windows.Front(); e != nil; {
		win := e.Value.(W)
		next := e.Next()

		// break, if we find a window with end time > watermark
		if win.EndTime().After(wm) {
			break
		}

		s.windows.Remove(e)
		closedWindows = append(closedWindows, win)
		e = next
	}

	return closedWindows
}

func (s *SortedWindowList[W]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.windows.Len()
}

func (s *SortedWindowList[W]) Front() W {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.windows.Front().Value.(W)
}

func (s *SortedWindowList[W]) Back() W {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.windows.Back().Value.(W)
}

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

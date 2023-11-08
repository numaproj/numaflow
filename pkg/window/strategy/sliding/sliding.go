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
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// Window TimedWindow implementation for Sliding window.
type Window struct {
	startTime time.Time
	endTime   time.Time
	slot      string
}

func NewWindow(startTime time.Time, endTime time.Time, message *isb.ReadMessage) window.TimedWindow {
	// TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the key
	// slot := slotAssigner.AssignSlot(message.Key)
	// for now we are using slot-0
	slot := "slot-0"
	return &Window{
		startTime: startTime,
		endTime:   endTime,
		slot:      slot,
	}
}

func (w *Window) StartTime() time.Time {
	return w.startTime
}

func (w *Window) EndTime() time.Time {
	return w.endTime
}

func (w *Window) Slot() string {
	return w.slot
}

func (w *Window) Partition() *partition.ID {
	return &partition.ID{
		Start: w.startTime,
		End:   w.endTime,
		Slot:  w.slot,
	}
}

// Merge merges the given window with the current window.
func (w *Window) Merge(tw window.TimedWindow) {
	if w.slot != tw.Slot() {
		panic("cannot merge windows with different slots")
	}
	// expand the start and end to accommodate the new window
	if tw.StartTime().Before(w.startTime) {
		w.startTime = tw.StartTime()
	}

	if tw.EndTime().After(w.endTime) {
		w.endTime = tw.EndTime()
	}
}

func (w *Window) Expand(endTime time.Time) {
	if endTime.After(w.endTime) {
		w.endTime = endTime
	}
}

// Windower is a implementation of TimedWindower of fixed window, windower is responsible for assigning
// windows to the incoming messages and closing the windows that are past the watermark.
type Windower struct {
	// Length is the temporal length of the window.
	length  time.Duration
	slide   time.Duration
	entries *window.SortedWindowList[window.TimedWindow]
}

func NewWindower(length time.Duration, slide time.Duration) window.TimedWindower {
	return &Windower{
		length: length,
		slide:  slide,
	}
}

// AssignWindows assigns the event to the window based on give window configuration.
// AssignWindows returns a map of partition id to window message. Partition id is used to
// identify the pbq instance to which the message should be assigned. Window message contains
// the isb message and the window operation. Window operation contains the event type and the
// if the window is newly created the operation is set to Create, if the window is already present
// the operation is set to Append.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowOperation {
	windowOperations := make([]*window.TimedWindowOperation, 0)

	// use the highest integer multiple of slide length which is less than the eventTime
	// as the start time for the window. For example if the eventTime is 810 and slide
	// length is 70, use 770 as the startTime of the window. In that way we can be guarantee
	// consistency while assigning the messages to the windows.
	startTime := time.UnixMilli((message.EventTime.UnixMilli() / w.slide.Milliseconds()) * w.slide.Milliseconds())
	endTime := startTime.Add(w.length)

	// startTime and endTime will be the largest timestamp window for the given eventTime,
	// using that we can create other windows by subtracting the slide length

	// since there is overlap at the boundaries
	// we attribute the element to the window to the right (higher)
	// of the boundary
	// left exclusive and right inclusive
	// so given windows 500-600 and 600-700 and the event time is 600
	// we will add the element to 600-700 window and not to the 500-600 window.
	for !startTime.After(message.EventTime) && endTime.After(message.EventTime) {
		win, isPresent := w.entries.InsertIfNotPresent(NewWindow(startTime, endTime, message))
		operation := &window.TimedWindowOperation{
			IsbMessage: message,
			Event:      window.Append,
			Windows:    []window.TimedWindow{win},
			ID:         win.Partition(),
		}
		if !isPresent {
			operation.Event = window.Create
		}
		windowOperations = append(windowOperations, operation)
		startTime = startTime.Add(-w.slide)
		endTime = endTime.Add(-w.slide)

	}
	return windowOperations
}

// CloseWindows closes the windows that are past the watermark.
// CloseWindows returns a map of partition id to window message which should be closed.
// Partition id is used to identify the pbq instance to which the message should be assigned.
// Window message contains operation. Window operation contains the delete event type.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowOperation {
	windowOperations := make([]*window.TimedWindowOperation, 0)
	closedWindows := w.entries.RemoveWindows(time)
	for _, win := range closedWindows {
		operation := &window.TimedWindowOperation{
			IsbMessage: nil,
			Event:      window.Delete,
			Windows:    []window.TimedWindow{win},
			ID:         win.Partition(),
		}
		windowOperations = append(windowOperations, operation)
	}
	return windowOperations
}

// NextWindowToBeClosed returns the next window yet to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return w.entries.Front()
}

//// Sliding implements sliding windows
//type Sliding struct {
//	// Length is the duration of the window
//	Length time.Duration
//	// offset between successive windows.
//	// successive windows are phased out by this duration.
//	Slide time.Duration
//	// entries is the list of active windows that are currently being tracked.
//	// windows are sorted in chronological order with the earliest window at the head of the list.
//	// list.List is implemented as a doubly linked list which allows us to traverse the nodes in
//	// both the directions.
//	// Although the worst case time complexity is O(n), because of the time based ordering and
//	// since the elements are rarely out of order, the amortized complexity works out to be closer to O(1)
//	// Because most of the keys are expected to be associated with the most recent window, we always start
//	// the traversal from the tail of the list for Get and Create Operations. For Remove Operations, since
//	// the earlier windows are expected to be closed before the more recent ones, we start the traversal
//	// from the Head.
//	entries *window.SortedWindowList[window.AlignedKeyedWindower]
//}
//
//var _ window.Windower = (*Sliding)(nil)
//
//// NewSliding returns a Sliding windower
//func NewSliding(length time.Duration, slide time.Duration) *Sliding {
//	return &Sliding{
//		Length:  length,
//		Slide:   slide,
//		entries: window.NewSortedWindowList[window.AlignedKeyedWindower](),
//	}
//}
//
//// AssignWindow returns a set of windows that contain the element based on event time
//func (s *Sliding) AssignWindow(eventTime time.Time) []window.AlignedKeyedWindower {
//	windows := make([]window.AlignedKeyedWindower, 0)
//
//	// use the highest integer multiple of slide length which is less than the eventTime
//	// as the start time for the window. For example if the eventTime is 810 and slide
//	// length is 70, use 770 as the startTime of the window. In that way we can be guarantee
//	// consistency while assigning the messages to the windows.
//	startTime := time.UnixMilli((eventTime.UnixMilli() / s.Slide.Milliseconds()) * s.Slide.Milliseconds())
//	endTime := startTime.Add(s.Length)
//
//	// startTime and endTime will be the largest timestamp window for the given eventTime,
//	// using that we can create other windows by subtracting the slide length
//
//	// since there is overlap at the boundaries
//	// we attribute the element to the window to the right (higher)
//	// of the boundary
//	// left exclusive and right inclusive
//	// so given windows 500-600 and 600-700 and the event time is 600
//	// we will add the element to 600-700 window and not to the 500-600 window.
//	for !startTime.After(eventTime) && endTime.After(eventTime) {
//		windows = append(windows, keyed.NewKeyedWindow(startTime, endTime))
//		startTime = startTime.Add(-s.Slide)
//		endTime = endTime.Add(-s.Slide)
//	}
//
//	return windows
//
//}
//
//// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window
//func (s *Sliding) InsertIfNotPresent(kw window.AlignedKeyedWindower) (window.AlignedKeyedWindower, bool) {
//	return s.entries.InsertIfNotPresent(kw)
//}
//
//func (s *Sliding) RemoveWindows(wm time.Time) []window.AlignedKeyedWindower {
//	return s.entries.RemoveWindows(wm)
//}
//
//// NextWindowToBeClosed returns the next window which is yet to be closed.
//func (s *Sliding) NextWindowToBeClosed() window.AlignedKeyedWindower {
//	if s.entries.Len() == 0 {
//		return nil
//	}
//	return s.entries.Front()
//}

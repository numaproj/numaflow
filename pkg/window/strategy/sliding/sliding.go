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

// slidingWindow TimedWindow implementation for Sliding window.
type slidingWindow struct {
	startTime time.Time
	endTime   time.Time
	slot      string
}

func NewSlidingWindow(startTime time.Time, endTime time.Time) window.TimedWindow {
	// TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the key
	// slot := slotAssigner.AssignSlot(message.Key)
	// for now we are using slot-0
	slot := "slot-0"
	return &slidingWindow{
		startTime: startTime,
		endTime:   endTime,
		slot:      slot,
	}
}

var _ window.TimedWindow = (*slidingWindow)(nil)

func (w *slidingWindow) StartTime() time.Time {
	return w.startTime
}

func (w *slidingWindow) EndTime() time.Time {
	return w.endTime
}

func (w *slidingWindow) Slot() string {
	return w.slot
}

func (w *slidingWindow) Keys() []string {
	return nil
}

func (w *slidingWindow) Partition() *partition.ID {
	return &partition.ID{
		Start: w.startTime,
		End:   w.endTime,
		Slot:  w.slot,
	}
}

// Merge merges the given window with the current window.
func (w *slidingWindow) Merge(_ window.TimedWindow) {
	// never be invoked for Aligned Window
}

func (w *slidingWindow) Expand(_ time.Time) {
	// never be invoked for Aligned Window
}

// Windower is an implementation of TimedWindower of sliding window, windower is responsible for assigning
// windows to the incoming messages and closing the windows that are past the watermark.
type Windower struct {
	// Length is the temporal length of the window.
	length time.Duration
	slide  time.Duration
	//activeWindows is a list of active windows which are yet to be closed
	activeWindows *window.SortedWindowListByEndTime
	// closedWindows is a list of closed windows which are yet to be GCed
	// we need to track the close windows because while publishing the watermark
	// for a sliding window, we need to compare the watermark with the oldest closed window
	closedWindows *window.SortedWindowListByEndTime
}

func NewWindower(length time.Duration, slide time.Duration) window.TimedWindower {
	return &Windower{
		length:        length,
		slide:         slide,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}
}

var _ window.TimedWindower = (*Windower)(nil)

func (w *Windower) Strategy() window.Strategy {
	return window.Sliding
}

// Type implements window.TimedWindower.
func (*Windower) Type() window.Type {
	return window.Aligned
}

// AssignWindows assigns the event to the window based on give window configuration.
// For sliding window, the message can be assigned to multiple windows.
// The operation can be either OPEN or APPEND, depending on whether the window is already present or not.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	windowOperations := make([]*window.TimedWindowRequest, 0)

	// use the highest integer multiple of slide length which is less than the eventTime
	// as the start time for the window. For example, if the eventTime is 810 and slide
	// length is 70, use 770 as the startTime of the window. In that way, we can guarantee
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
		win, isPresent := w.activeWindows.InsertIfNotPresent(NewSlidingWindow(startTime, endTime))

		op := window.Open
		if isPresent {
			op = window.Append
		}

		operation := &window.TimedWindowRequest{
			ReadMessage: message,
			Operation:   op,
			Windows:     []window.TimedWindow{win},
			ID:          win.Partition(),
		}

		windowOperations = append(windowOperations, operation)
		startTime = startTime.Add(-w.slide)
		endTime = endTime.Add(-w.slide)
	}

	return windowOperations
}

// InsertWindow inserts a window to the list of active windows.
func (w *Windower) InsertWindow(tw window.TimedWindow) {
	w.activeWindows.InsertIfNotPresent(tw)
}

// CloseWindows closes the windows that are past the watermark.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowRequest {
	windowOperations := make([]*window.TimedWindowRequest, 0)
	closedWindows := w.activeWindows.RemoveWindows(time)
	for _, win := range closedWindows {
		operation := &window.TimedWindowRequest{
			ReadMessage: nil,
			Operation:   window.Delete,
			Windows:     []window.TimedWindow{win},
			ID:          win.Partition(),
		}
		windowOperations = append(windowOperations, operation)
		w.closedWindows.InsertBack(win)
	}
	return windowOperations
}

// NextWindowToBeClosed returns the next window yet to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return w.activeWindows.Front()
}

// DeleteClosedWindow deletes the window from the closed windows list
func (w *Windower) DeleteClosedWindow(response *window.TimedWindowResponse) {
	w.closedWindows.Delete(response.Window)
}

// OldestWindowEndTime returns the end time of the oldest window among both active and closed windows.
// If there are no windows, it returns -1.
func (w *Windower) OldestWindowEndTime() time.Time {
	if win := w.closedWindows.Front(); win != nil {
		return win.EndTime()
	} else if win = w.activeWindows.Front(); win != nil {
		return win.EndTime()
	} else {
		return time.UnixMilli(-1)
	}
}

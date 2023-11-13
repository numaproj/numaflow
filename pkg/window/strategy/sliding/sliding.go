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
	keys      []string
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
		keys:      message.Keys,
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

func (w *Window) Keys() []string {
	return w.keys
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
	length time.Duration
	slide  time.Duration
	//activeWindows is a list of active windows which are yet to be closed
	activeWindows *window.SortedWindowListByEndTime[window.TimedWindow]
	// closedWindows is a list of closed windows which are yet to be GCed
	// we need to track the close windows because while publishing the watermark
	// for a fixed window, we need to compare the watermark with the oldest closed window
	closedWindows *window.SortedWindowListByEndTime[window.TimedWindow]
}

func NewWindower(length time.Duration, slide time.Duration) window.TimedWindower {
	return &Windower{
		length:        length,
		slide:         slide,
		activeWindows: window.NewSortedWindowListByEndTime[window.TimedWindow](),
		closedWindows: window.NewSortedWindowListByEndTime[window.TimedWindow](),
	}
}

// NewWindowFromPartition returns a new window for the given partition.
func NewWindowFromPartition(id *partition.ID) window.TimedWindow {
	return &Window{
		startTime: id.Start,
		endTime:   id.End,
		slot:      id.Slot,
	}
}

func (w *Windower) Strategy() window.Strategy {
	return window.Sliding
}

// AssignWindows assigns the event to the window based on give window configuration.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	windowOperations := make([]*window.TimedWindowRequest, 0)

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
		win, isPresent := w.activeWindows.InsertIfNotPresent(NewWindow(startTime, endTime, message))
		operation := &window.TimedWindowRequest{
			ReadMessage: message,
			Operation:   window.Append,
			Windows:     []window.TimedWindow{win},
			ID:          win.Partition(),
		}
		if !isPresent {
			operation.Operation = window.Open
		}
		windowOperations = append(windowOperations, operation)
		startTime = startTime.Add(-w.slide)
		endTime = endTime.Add(-w.slide)

	}
	return windowOperations
}

// InsertWindow inserts window to the list of active windows
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

// DeleteClosedWindows deletes the windows from the closed windows list
func (w *Windower) DeleteClosedWindows(response *window.TimedWindowResponse) {
	w.closedWindows.Delete(&Window{
		startTime: response.ID.Start,
		endTime:   response.ID.End,
		slot:      response.ID.Slot,
	})
}

// OldestClosedWindowEndTime returns the end time of the oldest closed window.
func (w *Windower) OldestClosedWindowEndTime() time.Time {
	if win := w.closedWindows.Front(); win != nil {
		return win.EndTime()
	} else {
		return time.UnixMilli(-1)
	}
}

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

// Package fixed implements Fixed windows. Fixed windows (sometimes called tumbling windows) are
// defined by a static window size, e.g. minutely windows or hourly windows. They are generally aligned, i.e. every
// window applies across all the data for the corresponding period of time.
// Package fixed also maintains the state of active keyed windows in a vertex.
// Keyed AlignedWindower maintains the association between set of keys and an interval window.
// keyed also provides the lifecycle management of an interval window. Watermark is used to trigger the expiration of windows.
package fixed

import (
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// fixedWindow TimedWindow implementation for Fixed window.
type fixedWindow struct {
	startTime time.Time
	endTime   time.Time
	slot      string
}

// NewFixedWindow returns a new window for the given message.
func NewFixedWindow(length time.Duration, message *isb.ReadMessage) window.TimedWindow {
	start := message.EventTime.Truncate(length)
	end := start.Add(length)
	// TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the key
	slot := "slot-0"
	return &fixedWindow{
		startTime: start,
		endTime:   end,
		slot:      slot,
	}
}

var _ window.TimedWindow = (*fixedWindow)(nil)

func (w *fixedWindow) StartTime() time.Time {
	return w.startTime
}

func (w *fixedWindow) EndTime() time.Time {
	return w.endTime
}

func (w *fixedWindow) Slot() string {
	return w.slot
}

func (w *fixedWindow) Keys() []string {
	return nil
}

func (w *fixedWindow) Partition() *partition.ID {
	return &partition.ID{
		Start: w.startTime,
		End:   w.endTime,
		Slot:  w.slot,
	}
}

func (w *fixedWindow) Merge(_ window.TimedWindow) {
	// never be invoked for Aligned Window
}

func (w *fixedWindow) Expand(_ time.Time) {
	// never be invoked for Aligned Window
}

// Windower is an implementation of TimedWindower of fixed window, windower is responsible for assigning
// windows to the incoming messages and closing the windows that are past the watermark.
type Windower struct {
	// Length is the temporal length of the window.
	length time.Duration
	// we track all the active windows, we store the windows sorted by end time
	// so it's easy to find the window
	activeWindows *window.SortedWindowListByEndTime
	// closedWindows is a list of closed windows which are yet to be GCed
	// we need to track the close windows because while publishing the watermark
	// for a fixed window, we need to compare the watermark with the oldest closed window
	closedWindows *window.SortedWindowListByEndTime
}

func NewWindower(length time.Duration) window.TimedWindower {
	return &Windower{
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
		length:        length,
	}
}

var _ window.TimedWindower = (*Windower)(nil)

func (w *Windower) Strategy() window.Strategy {
	return window.Fixed
}

func (w *Windower) Type() window.Type {
	return window.Aligned
}

// AssignWindows assigns the event to the window based on give window configuration.
// For fixed window, the message is assigned to one single window.
// The operation can be either OPEN or APPEND, depending on whether the window is already present or not.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	win, isPresent := w.activeWindows.InsertIfNotPresent(NewFixedWindow(w.length, message))

	op := window.Open
	if isPresent {
		op = window.Append
	}

	winOp := &window.TimedWindowRequest{
		Operation:   op,
		Windows:     []window.TimedWindow{win},
		ReadMessage: message,
		ID:          win.Partition(),
	}

	return []*window.TimedWindowRequest{winOp}
}

// InsertWindow inserts a window to the list of active windows
func (w *Windower) InsertWindow(tw window.TimedWindow) {
	w.activeWindows.InsertIfNotPresent(tw)
}

// CloseWindows closes the windows that are past the watermark.
// returns a list of TimedWindowRequests, each request contains the window operation and the window
// which needs to be closed.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowRequest {
	// FIXME - we are updating both active and closed windows.
	// We need to make this method atomic.
	// Same for other windower methods.
	winOperations := make([]*window.TimedWindowRequest, 0)
	closedWindows := w.activeWindows.RemoveWindows(time)
	for _, win := range closedWindows {
		winOp := &window.TimedWindowRequest{
			ReadMessage: nil,
			// we can call Delete here because in an aligned window, we are sure that COB has been called for all the keys
			Operation: window.Delete,
			Windows:   []window.TimedWindow{win},
			ID:        win.Partition(),
		}
		winOperations = append(winOperations, winOp)
		w.closedWindows.InsertBack(win)
	}
	return winOperations
}

// NextWindowToBeClosed returns the next window yet to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return w.activeWindows.Front()
}

// DeleteClosedWindow deletes the window from the closed windows list.
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

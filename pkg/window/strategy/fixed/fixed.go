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

// Window TimedWindow implementation for Fixed window.
type Window struct {
	startTime time.Time
	endTime   time.Time
	slot      string
}

func NewWindow(length time.Duration, message *isb.ReadMessage) window.TimedWindow {
	start := message.EventTime.Truncate(length)
	end := start.Add(length)
	// TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the key
	slot := "slot-0"
	return &Window{
		startTime: start,
		endTime:   end,
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
	entries *window.SortedWindowList[window.TimedWindow]
}

func NewWindower(length time.Duration) window.TimedWindower {
	return &Windower{
		length: length,
	}
}

// AssignWindows assigns the event to the window based on give window configuration.
// AssignWindows returns a map of partition id to window message. Partition id is used to
// identify the pbq instance to which the message should be assigned. Window message contains
// the isb message and the window operation. Window operation contains the event type and the
// if the window is newly created the operation is set to Create, if the window is already present
// the operation is set to Append.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowOperation {
	win, isPresent := w.entries.InsertIfNotPresent(NewWindow(w.length, message))
	winOp := &window.TimedWindowOperation{
		Event:      window.Append,
		Windows:    []window.TimedWindow{win},
		IsbMessage: message,
		ID:         win.Partition(),
	}
	if !isPresent {
		winOp.Event = window.Create
	}
	return []*window.TimedWindowOperation{winOp}
}

// CloseWindows closes the windows that are past the watermark.
// CloseWindows returns a map of partition id to window message which should be closed.
// Partition id is used to identify the pbq instance to which the message should be assigned.
// Window message contains operation. Window operation contains the delete event type.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowOperation {
	winOperations := make([]*window.TimedWindowOperation, 0)
	closedWindows := w.entries.RemoveWindows(time)
	for _, win := range closedWindows {
		winOp := &window.TimedWindowOperation{
			IsbMessage: nil,
			Event:      window.Delete,
			Windows:    []window.TimedWindow{win},
			ID:         win.Partition(),
		}
		winOperations = append(winOperations, winOp)
	}
	return winOperations
}

// NextWindowToBeClosed returns the next window yet to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return w.entries.Front()
}

//// Fixed implements Fixed window.
//// Fixed maintains the state of active windows
//// All the operations in Fixed (see window.Windower) order the entries in the ascending order of start time.
//// So the earliest window is at the front and the oldest window is at the end.
//type Fixed struct {
//	// Length is the temporal length of the window.
//	Length time.Duration
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
//var _ window.Windower = (*Fixed)(nil)
//
//// NewFixed returns a Fixed windower.
//func NewFixed(length time.Duration) window.Windower {
//	return &Fixed{
//		Length:  length,
//		entries: window.NewSortedWindowList[window.AlignedKeyedWindower](),
//	}
//}
//
//// AssignWindow assigns a window for the given eventTime.
//func (f *Fixed) AssignWindow(eventTime time.Time) []window.AlignedKeyedWindower {
//	start := eventTime.Truncate(f.Length)
//	end := start.Add(f.Length)
//
//	// Assignment of windows should follow a Left inclusive and right exclusive
//	// principle. Since we use truncate here, it is guaranteed that any element
//	// on the boundary will automatically fall in to the window to the right
//	// of the boundary thereby satisfying the requirement.
//	return []window.AlignedKeyedWindower{
//		keyed.NewKeyedWindow(start, end),
//	}
//}
//
//// InsertIfNotPresent inserts a window to the list of active windows if not present and returns the window
//func (f *Fixed) InsertIfNotPresent(kw window.AlignedKeyedWindower) (aw window.AlignedKeyedWindower, isPresent bool) {
//	return f.entries.InsertIfNotPresent(kw)
//}
//
//// RemoveWindows returns an array of keyed windows that are before the current watermark.
//// So these windows can be closed.
//func (f *Fixed) RemoveWindows(wm time.Time) []window.AlignedKeyedWindower {
//	return f.entries.RemoveWindows(wm)
//}
//
//// NextWindowToBeClosed returns the next window which is yet to be closed.
//func (f *Fixed) NextWindowToBeClosed() window.AlignedKeyedWindower {
//	if f.entries.Len() == 0 {
//		return nil
//	}
//	return f.entries.Front()
//}

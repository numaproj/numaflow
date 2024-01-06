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

package session

import (
	"math"
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

const delimiter = ":"

// SharedSessionPartition is a common partition for session window.
// session windows share a common pbq, this partition is used to identify the pbq instance.
var SharedSessionPartition = partition.ID{
	Start: time.UnixMilli(0),
	End:   time.UnixMilli(math.MaxInt64),
	Slot:  "slot-0",
}

// sessionWindow TimedWindow implementation for Session window.
type sessionWindow struct {
	startTime time.Time
	endTime   time.Time
	slot      string
	keys      []string
}

func NewSessionWindow(startTime time.Time, gap time.Duration, message *isb.ReadMessage) window.TimedWindow {
	start := startTime
	end := start.Add(gap)
	// TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the keys
	slot := "slot-0"
	return &sessionWindow{
		startTime: start,
		endTime:   end,
		slot:      slot,
		keys:      message.Keys,
	}
}

var _ window.TimedWindow = (*sessionWindow)(nil)

func (w *sessionWindow) StartTime() time.Time {
	return w.startTime
}

func (w *sessionWindow) EndTime() time.Time {
	return w.endTime
}

func (w *sessionWindow) Slot() string {
	return w.slot
}

func (w *sessionWindow) Keys() []string {
	return w.keys
}

func (w *sessionWindow) Partition() *partition.ID {
	return &partition.ID{
		Start: w.startTime,
		End:   w.endTime,
		Slot:  w.slot,
	}
}

// Merge window merges two windows. This operation of merge happens in Unaligned windows.
func (w *sessionWindow) Merge(tw window.TimedWindow) {
	// expand the start and end to accommodate the new window
	if tw.StartTime().Before(w.startTime) {
		w.startTime = tw.StartTime()
	}

	if tw.EndTime().After(w.endTime) {
		w.endTime = tw.EndTime()
	}
}

// cloneWindow makes a deep copy of the window.
func cloneWindow(win window.TimedWindow) *sessionWindow {
	return &sessionWindow{
		startTime: win.StartTime(),
		endTime:   win.EndTime(),
		slot:      win.Slot(),
		keys:      win.Keys(),
	}
}

// Expand expands the window end time to the new endTime. An interesting property of Unaligned windows :).
func (w *sessionWindow) Expand(endTime time.Time) {
	if endTime.After(w.endTime) {
		w.endTime = endTime
	}
}

// Windower is an implementation of TimedWindower of session window, windower is responsible for assigning
// windows to the incoming messages and closing the windows that are past the watermark.
type Windower struct {
	// gap is the duration of inactivity after which a session window is marked as closed.
	gap time.Duration

	// activeWindows is a map of keys to list of active windows
	// key is join of all the keys of the message, since session is per key
	// we need to maintain a list of windows per key
	activeWindows map[string]*window.SortedWindowListByEndTime

	// closedWindows is a list of closed windows which are yet to be GCed
	// we need to track the close windows because while publishing the watermark
	// for session window, we need to compare the watermark with the oldest closed window
	closedWindows *window.SortedWindowListByEndTime
}

func NewWindower(gap time.Duration) window.TimedWindower {
	return &Windower{
		gap:           gap,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}
}

var _ window.TimedWindower = (*Windower)(nil)

// Strategy returns the window strategy
func (w *Windower) Strategy() window.Strategy {
	return window.Session
}

// Type implements window.TimedWindower.
func (*Windower) Type() window.Type {
	return window.Unaligned
}

// AssignWindows assigns the event to the window based on give window configuration. This assignment could trigger the following
// - New window Creation
// - Expand an existing window
// - Append to an existing window (the message has the event-time such that gap + event-time is < window end time).
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	var (
		combinedKey      = strings.Join(message.Keys, delimiter)
		windowOperations = make([]*window.TimedWindowRequest, 0)
		win              = NewSessionWindow(message.EventTime, w.gap, message)
	)

	// check whether we have any active windows created for this key
	// NOTE: we track per key windows for Unaligned.
	list, ok := w.activeWindows[combinedKey]

	// if there is no existing windows for that key, we create a new window and
	// update the activeWindows map, and then return early
	if !ok {
		list = window.NewSortedWindowListByEndTime()
		// since it's the first window, we can insert it at the front
		list.InsertFront(win)
		windowOperations = append(windowOperations, createWindowOperation(message, window.Open, []window.TimedWindow{win}, &SharedSessionPartition))
		w.activeWindows[combinedKey] = list

		return windowOperations
	}

	// now that we know that there are active windows for this key, we need to check
	// if there are any compatible windows for the key, if yes we can append the message
	// to any compatible window or that window can be expanded to accommodate the message
	// if we can't append or expand, we need to create a new window
	// NOTE to reader: what is compatible? :)

	// we check if the new window created by the message can be merged
	// with any existing window, WindowToBeMerged returns the window that can be merged
	// if the returned window is not same as the window created by the message, that means
	// we need to expand the window to accommodate the new message and send an expand operation
	// to the server
	if windowToBeMerged, canBeMerged := list.WindowToBeMerged(win); canBeMerged {
		// check whether we have to do expand window.
		if win.StartTime().Before(windowToBeMerged.StartTime()) || win.EndTime().After(windowToBeMerged.EndTime()) {
			// we need to clone the window because for expand operation we need to send the old window (e.g., 60-70)
			// and the expanded window (e.g., 60-72).
			oldWindow := cloneWindow(windowToBeMerged)

			// update the reference of the window in the list with the expanded window
			windowToBeMerged.Merge(win)
			windowOperations = append(windowOperations, createWindowOperation(message, window.Expand, []window.TimedWindow{oldWindow, windowToBeMerged}, &SharedSessionPartition))
		} else { // no need to expand, message doesn't change the window end-time.
			// if the returned window is same as the window created by the message, that means
			// the message belongs to an existing window and it's an append operation
			windowOperations = append(windowOperations, createWindowOperation(message, window.Append, []window.TimedWindow{windowToBeMerged}, &SharedSessionPartition))
		}

		return windowOperations
	}

	// if the window cannot be merged, that means we need to create a new window to the active windows list
	list.Insert(win)
	windowOperations = append(windowOperations, createWindowOperation(message, window.Open, []window.TimedWindow{win}, &SharedSessionPartition))

	return windowOperations
}

// InsertWindow inserts a window to the list of active windows.
func (w *Windower) InsertWindow(tw window.TimedWindow) {
	combinedKey := strings.Join(tw.Keys(), delimiter)
	if list, ok := w.activeWindows[combinedKey]; !ok {
		list = window.NewSortedWindowListByEndTime()
		list.InsertFront(tw)
		w.activeWindows[combinedKey] = list
	} else {
		list.Insert(tw)
	}
}

func createWindowOperation(message *isb.ReadMessage, event window.Operation, windows []window.TimedWindow, id *partition.ID) *window.TimedWindowRequest {
	// clone the windows because the windows might be updated after the operation is sent to the server.
	// we do in-place updates, but those will be merged later on, hence correctness won't be affected.
	var clonedWindows = make([]window.TimedWindow, 0)
	for _, win := range windows {
		clonedWindows = append(clonedWindows, cloneWindow(win))
	}
	return &window.TimedWindowRequest{
		ReadMessage: message,
		Operation:   event,
		Windows:     clonedWindows,
		ID:          id,
	}
}

// CloseWindows closes the windows that are past the watermark
// and also merges the windows that should be merged.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowRequest {

	windowOperations := make([]*window.TimedWindowRequest, 0)

	for _, list := range w.activeWindows {
		closedWindows := list.RemoveWindows(time)

		// nothing to close
		if len(closedWindows) == 0 {
			continue
		}

		// check if any of the windows that is being close can be merged with another window (active or otherwise)
		// for example if we have two session windows for a key  (60, 78) and (75, 85)
		// we should merge them to (60, 85)
		mergedWindows := windowsThatCanBeMerged(closedWindows)
		for _, windows := range mergedWindows {
			// windowsThatCanBeMerged groups the windows that can be merged into a slice
			// if there are more than one window, that means we need to merge them
			// so we need to send a merge operation to the server
			if len(windows) > 1 {

				// make UDF aware that windows have to be merged
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Merge, windows, &SharedSessionPartition))

				// merge the first window with subsequent windows and it grows as it merges
				var mergedWindow = cloneWindow(windows[0])
				for _, win := range windows[1:] {
					mergedWindow.Merge(win)
				}

				// before closing the mergedWindow we need to check if it can be merged with any of the
				// existing activeWindows, for example if we have two closed session windows (60, 70) and
				// (65, 75) and one active window (73, 83).
				// We should merge the closed windows (60,70) and (65,75) to the new bigger closed window (60, 75).
				// Now we have to merge this bigger closed window with the active window
				// (73, 83) to create an even bigger active window (60, 83).
				// whew :-P
				toBeMerged, canBeMerged := list.WindowToBeMerged(mergedWindow)
				windowOperations = append(windowOperations, w.handleWindowToBeMerged(canBeMerged, toBeMerged, mergedWindow))
			} else {
				// since there is only one window, we can check if it can be merged with any of the active windows
				// if it can be merged, we should merge it and send a merge operation
				// if it can't be merged, we should close it and send a close operation
				toBeMerged, canBeMerged := list.WindowToBeMerged(windows[0])
				windowOperations = append(windowOperations, w.handleWindowToBeMerged(canBeMerged, toBeMerged, windows[0]))
			}
		}
	}

	return windowOperations
}

// handleWindowToBeMerged sends a merge operation if the window can be merged with any of the active windows
// otherwise it sends a close operation
func (w *Windower) handleWindowToBeMerged(canBeMerged bool, toBeMerged window.TimedWindow, latestWin window.TimedWindow) *window.TimedWindowRequest {
	if canBeMerged {
		oldWindow := cloneWindow(toBeMerged)
		toBeMerged.Merge(latestWin)
		return createWindowOperation(nil, window.Merge, []window.TimedWindow{latestWin, oldWindow}, &SharedSessionPartition)
	}

	w.closedWindows.Insert(latestWin)
	return createWindowOperation(nil, window.Close, []window.TimedWindow{latestWin}, &SharedSessionPartition)
}

// NextWindowToBeClosed returns the next window yet to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return &sessionWindow{
		startTime: SharedSessionPartition.Start,
		endTime:   SharedSessionPartition.End,
		slot:      SharedSessionPartition.Slot,
	}
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
	}

	var minEndTime = time.UnixMilli(-1)
	for _, windows := range w.activeWindows {
		if win := windows.Front(); win != nil && (minEndTime.UnixMilli() == -1 || win.EndTime().Before(minEndTime)) {
			minEndTime = win.EndTime()
		}
	}

	return minEndTime
}

// windowsThatCanBeMerged is a function that takes a slice of windows (each window defined by a start and end time)
// and returns a slice of slices of windows that can be merged based on their overlapping times.
// A window can be merged with another if its end time is after the start time of the next window.
//
// For example, given the windows (75, 85), (60, 90), (80, 100) and (110, 120),
// the function returns [][]window.TimedWindow{{(60, 90), (75, 85), (80, 100)}, {(110, 120)}}
// because the first three windows overlap and can be merged, while the last window stands alone.
func windowsThatCanBeMerged(windows []window.TimedWindow) [][]window.TimedWindow {
	// If there are no windows, return nil
	if len(windows) == 0 {
		return nil
	}

	// Initialize an empty slice to hold slices of mergeable windows
	mWindows := make([][]window.TimedWindow, 0)

	i := len(windows) - 1
	// Reverse iterate over the windows because it is sorted by end-time.
	for i >= 0 {
		// Initialize a slice to hold the current window and any subsequent mergeable windows
		merged := []window.TimedWindow{windows[i]}

		// Set the last window to be the current window
		last := cloneWindow(windows[i])

		// now that i have stored the last, let's skip it
		i--

		// Check if the end time of the last window is after the start time of the previous window
		// If it is that means they should be merged, add the previous window to the merged slice
		// and update the end time of the last window
		for i >= 0 && windows[i].EndTime().After(last.StartTime()) {
			merged = append(merged, windows[i])
			last.Merge(windows[i])
			i--
		}

		// Add the merged slice to the slice of all mergeable windows
		mWindows = append(mWindows, merged)
	}

	// Return the slice of all mergeable windows
	return mWindows
}

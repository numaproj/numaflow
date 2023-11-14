package session

import (
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

var delimiter = ":"

// GlobalPartition is a global partition for session window.
// session windows share a common pbq, this partition is used to identify the pbq instance.
var GlobalPartition = partition.ID{
	Start: time.UnixMilli(-1),
	End:   time.UnixMilli(-1),
	Slot:  "slot-0",
}

// Window TimedWindow implementation for Session window.
type Window struct {
	startTime time.Time
	endTime   time.Time
	slot      string
	keys      []string
}

func NewWindow(startTime time.Time, gap time.Duration, message *isb.ReadMessage) window.TimedWindow {
	start := startTime
	end := start.Add(gap)
	//TODO: slot should be extracted based on the key
	// we can accept an interface SlotAssigner
	// which will assign the slot based on the key
	slot := "slot-0"
	return &Window{
		startTime: start,
		endTime:   end,
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

func cloneWindow(win window.TimedWindow) *Window {
	return &Window{
		startTime: win.StartTime(),
		endTime:   win.EndTime(),
		slot:      win.Slot(),
		keys:      win.Keys(),
	}
}

func (w *Window) Expand(endTime time.Time) {
	if endTime.After(w.endTime) {
		w.endTime = endTime
	}
}

// Windower is a implementation of TimedWindower of session window, windower is responsible for assigning
// windows to the incoming messages and closing the windows that are past the watermark.
type Windower struct {
	gap time.Duration
	// activeWindows is a map of keys to list of active windows
	// key is join of all the keys of the message, since session is per key
	// we need to maintain a list of windows per key
	activeWindows map[string]*window.SortedWindowListByEndTime[window.TimedWindow]

	// closedWindows is a list of closed windows which are yet to be GCed
	// we need to track the close windows because while publishing the watermark
	// for session window, we need to compare the watermark with the oldest closed window
	closedWindows *window.SortedWindowListByEndTime[window.TimedWindow]
}

func NewWindower(gap time.Duration) window.TimedWindower {
	return &Windower{
		gap:           gap,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime[window.TimedWindow]),
		closedWindows: window.NewSortedWindowListByEndTime[window.TimedWindow](),
	}
}

// Strategy returns the window strategy
func (w *Windower) Strategy() window.Strategy {
	return window.Session
}

// AssignWindows assigns the event to the window based on give window configuration.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	sessionPartition := GlobalPartition

	// TODO: slot should be extracted based on the key
	sessionPartition.Slot = "slot-0"
	combinedKey := strings.Join(message.Keys, delimiter)
	windowOperations := make([]*window.TimedWindowRequest, 0)

	// if the window is not present, create a new window
	if list, ok := w.activeWindows[combinedKey]; !ok {
		win := NewWindow(message.EventTime, w.gap, message)
		list = window.NewSortedWindowListByEndTime[window.TimedWindow]()
		// since its the first window, we can insert it at the front
		list.InsertFront(win)
		windowOperations = append(windowOperations, createWindowOperation(message, window.Open, []window.TimedWindow{win}, &sessionPartition))
		w.activeWindows[combinedKey] = list
	} else {
		// if the window is present, check if the message belongs to any existing window
		// if the message belongs to an existing window, append the message to the window
		// check if the window needs to be expanded
		// example if we have window (60, 70) and if we get a message with event time 65
		// and gap duration is 10s we need to expand the window to (60, 75) end time will
		// be max of existing end time and message event time + gap
		win, isPresent := list.FindWindowForTime(message.EventTime)
		if isPresent {
			if win.EndTime().Before(message.EventTime.Add(w.gap)) {
				expandedWin := NewWindow(win.StartTime(), w.gap, message)
				expandedWin.Expand(message.EventTime.Add(w.gap))
				windowOperations = append(windowOperations, createWindowOperation(message, window.Expand, []window.TimedWindow{win, expandedWin}, &sessionPartition))
			} else {
				windowOperations = append(windowOperations, createWindowOperation(message, window.Append, []window.TimedWindow{win}, &sessionPartition))
			}
		} else {
			// if the message does not belong to any existing window, create a new window
			win = NewWindow(message.EventTime, w.gap, message)
			list.Insert(win)
			windowOperations = append(windowOperations, createWindowOperation(message, window.Open, []window.TimedWindow{win}, &sessionPartition))
		}
	}

	return windowOperations
}

// InsertWindow inserts window to the list of active windows
func (w *Windower) InsertWindow(tw window.TimedWindow) {
	combinedKey := strings.Join(tw.Keys(), delimiter)
	if list, ok := w.activeWindows[combinedKey]; !ok {
		list = window.NewSortedWindowListByEndTime[window.TimedWindow]()
		list.InsertFront(tw)
		w.activeWindows[combinedKey] = list
	} else {
		list.Insert(tw)
	}
}

func createWindowOperation(message *isb.ReadMessage, event window.Operation, windows []window.TimedWindow, id *partition.ID) *window.TimedWindowRequest {
	return &window.TimedWindowRequest{
		ReadMessage: message,
		Operation:   event,
		Windows:     windows,
		ID:          id,
	}
}

// CloseWindows closes the windows that are past the watermark.
// also merges the windows that should be merged
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowRequest {
	sessionPartition := GlobalPartition

	// TODO: slot should be extracted based on the key
	sessionPartition.Slot = "slot-0"

	windowOperations := make([]*window.TimedWindowRequest, 0)
	for _, list := range w.activeWindows {
		closedWindows := list.RemoveWindows(time)
		if len(closedWindows) == 0 {
			continue
		}
		// check if any of the windows can be merged
		// for example if we have two session windows for a key  (60, 78) and (75, 85)
		// we should merge them to (60, 85)
		mergedWindows := windowsThatCanBeMerged(closedWindows)
		for _, windows := range mergedWindows {
			// if there are more than one window, that means we need to merge them
			// so we need to send a merge operation to the server
			if len(windows) > 1 {
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Merge, windows, &sessionPartition))
				var mergedWindow = cloneWindow(windows[0])
				for _, win := range windows {
					mergedWindow.Merge(win)
				}
				// we should close the merged window, because we are merging the closed windows
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Close, []window.TimedWindow{mergedWindow}, &sessionPartition))
				w.closedWindows.Insert(mergedWindow)
			} else {
				// we should always close the first window
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Close, []window.TimedWindow{windows[0]}, &sessionPartition))
				w.closedWindows.Insert(windows[0])
			}
		}
	}

	return windowOperations
}

// NextWindowToBeClosed returns the next window yet to be closed.
// since session window is not based on time, we return a global window
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	return &Window{
		startTime: time.UnixMilli(-1),
		endTime:   time.UnixMilli(-1),
		slot:      "slot-0",
	}
}

func (w *Windower) DeleteClosedWindows(response *window.TimedWindowResponse) {
	w.closedWindows.Delete(&Window{
		startTime: response.ID.Start,
		endTime:   response.ID.End,
		slot:      response.ID.Slot,
	})
}

// OldestClosedWindowEndTime returns the end time of the oldest closed window.
// if there are no closed windows, it returns -1 as the end time
func (w *Windower) OldestClosedWindowEndTime() time.Time {
	if win := w.closedWindows.Front(); win != nil {
		return win.EndTime()
	} else {
		return time.UnixMilli(-1)
	}
}

// windowsThatCanBeMerged is a function that takes a slice of windows (each window defined by a start and end time)
// and returns a slice of slices of windows that can be merged based on their overlapping times.
// A window can be merged with another if its end time is after the start time of the next window.
//
// For example, given the windows (60, 90), (75, 85), (80, 100) and (110, 120),
// the function returns [][]window.TimedWindow{{(60, 90), (75, 85), (80, 100)}, {(110, 120)}}
// because the first three windows overlap and can be merged, while the last window stands alone.
func windowsThatCanBeMerged(windows []window.TimedWindow) [][]window.TimedWindow {
	// If there are no windows, return nil
	if len(windows) == 0 {
		return nil
	}

	// Initialize an empty slice to hold slices of mergeable windows
	mWindows := make([][]window.TimedWindow, 0)

	i := 0
	// Iterate over the windows
	for i < len(windows) {
		// Initialize a slice to hold the current window and any subsequent mergeable windows
		merged := []window.TimedWindow{windows[i]}
		// Set the first window to be the current window
		first := cloneWindow(windows[i])
		// Check if the end time of the first window is after the start time of the next window
		// If it is, add the next window to the merged slice and update the end time of the first window
		i++
		for i < len(windows) && first.EndTime().After(windows[i].StartTime()) {
			merged = append(merged, windows[i])
			first.Merge(windows[i])
			i++
		}
		// Add the merged slice to the slice of all mergeable windows
		mWindows = append(mWindows, merged)
	}
	// Return the slice of all mergeable windows
	return mWindows
}

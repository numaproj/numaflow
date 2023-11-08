package session

import (
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

var delimiter = ":"

var GlobalPartition = partition.ID{
	Start: time.UnixMilli(-1),
	End:   time.UnixMilli(-1),
	Slot:  "global",
}

// Window TimedWindow implementation for Session window.
type Window struct {
	startTime time.Time
	endTime   time.Time
	slot      string
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
	gap     time.Duration
	entries map[string]*window.SortedWindowList[window.TimedWindow]
}

func NewWindower(gap time.Duration) window.TimedWindower {
	return &Windower{
		gap:     gap,
		entries: make(map[string]*window.SortedWindowList[window.TimedWindow]),
	}
}

// AssignWindows assigns the event to the window based on give window configuration.
// AssignWindows returns a map of partition id to window message. Partition id is used to
// identify the pbq instance to which the message should be assigned. Window message contains
// the isb message and the window operation. Window operation contains the event type and the
// if the window is newly created the operation is set to Create, if the window is already present
// the operation is set to Append.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowOperation {
	sessionPartition := GlobalPartition

	// TODO: slot should be extracted based on the key
	sessionPartition.Slot = "slot-0"
	combinedKey := strings.Join(message.Keys, delimiter)
	windowOperations := make([]*window.TimedWindowOperation, 0)

	if list, ok := w.entries[combinedKey]; !ok {
		win := NewWindow(message.EventTime, w.gap, message)
		list = window.NewSortedWindowList[window.TimedWindow]()
		list.InsertFront(win)
		windowOperations = append(windowOperations, createWindowOperation(message, window.Create, []window.TimedWindow{win}, &sessionPartition))
		w.entries[combinedKey] = list
	} else {
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
			win = NewWindow(message.EventTime, w.gap, message)
			list.InsertFront(win)
			windowOperations = append(windowOperations, createWindowOperation(message, window.Create, []window.TimedWindow{win}, &sessionPartition))
		}
	}

	return windowOperations
}

func createWindowOperation(message *isb.ReadMessage, event window.Event, windows []window.TimedWindow, id *partition.ID) *window.TimedWindowOperation {
	return &window.TimedWindowOperation{
		IsbMessage: message,
		Event:      event,
		Windows:    windows,
		ID:         id,
	}
}

// CloseWindows closes the windows that are past the watermark.
// CloseWindows returns a map of partition id to window message which should be closed.
// Partition id is used to identify the pbq instance to which the message should be assigned.
// Window message contains operation. Window operation contains the delete event type.
func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowOperation {
	sessionPartition := GlobalPartition

	// TODO: slot should be extracted based on the key
	sessionPartition.Slot = "slot-0"

	windowOperations := make([]*window.TimedWindowOperation, 0)
	for _, list := range w.entries {
		closedWindows := list.RemoveWindows(time)
		mergedWindows := windowsThatCanBeMerged(closedWindows)
		for _, windows := range mergedWindows {
			if len(windows) == 1 {
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Close, windows, &sessionPartition))
			} else {
				windowOperations = append(windowOperations, createWindowOperation(nil, window.Merge, windows, &sessionPartition))
			}
		}
		// TODO: invoke merge op if there are any windows that are merged
		for _, win := range closedWindows {
			windowOperations = append(windowOperations, createWindowOperation(nil, window.Close, []window.TimedWindow{win}, &sessionPartition))
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

func windowsThatCanBeMerged(windows []window.TimedWindow) [][]window.TimedWindow {
	if len(windows) == 0 {
		return nil
	}
	mWindows := make([][]window.TimedWindow, 0)

	i := 0
	for i < len(windows) {
		merged := []window.TimedWindow{windows[i]}
		first := windows[i]
		for i+1 < len(mWindows) && first.EndTime().After(windows[i+1].StartTime()) {
			merged = append(merged, windows[i+1])
			first.Merge(windows[i+1])
			i++
		}
		mWindows = append(mWindows, merged)
	}
	return mWindows
}

package accumulate

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// accumulatorWindow TimedWindow implementation for Accumulator window.
type accumulatorWindow struct {
	// startTime will be 0 because this is kind of Global
	startTime time.Time
	// endTime will be MAX.Time since it could never end (because it is Global)
	endTime   time.Time
	slot      string
	keys      []string
	partition *partition.ID
	windowID  string
}

// NewAccumulatorWindow creates a new Accumulator window.
func NewAccumulatorWindow(keys []string) window.TimedWindow {
	slot := "slot-0"
	return &accumulatorWindow{
		startTime: time.Unix(0, 0),
		endTime:   time.Unix(0, math.MaxInt64),
		slot:      slot,
		keys:      keys,
		partition: &window.SharedUnalignedPartition,
		windowID:  fmt.Sprintf("%d-%d-%s-%s", 0, math.MaxInt64, slot, strings.Join(keys, dfv1.KeysDelimitter)),
	}
}

var _ window.TimedWindow = (*accumulatorWindow)(nil)

func (w *accumulatorWindow) StartTime() time.Time {
	return w.startTime
}

func (w *accumulatorWindow) EndTime() time.Time {
	return w.endTime
}

func (w *accumulatorWindow) Slot() string {
	return w.slot
}

func (w *accumulatorWindow) Keys() []string {
	return w.keys
}

func (w *accumulatorWindow) Partition() *partition.ID {
	return w.partition
}

func (w *accumulatorWindow) ID() string {
	return w.windowID
}

func (w *accumulatorWindow) Merge(window.TimedWindow) {
	// No merge operation for Accumulator window
}

func (w *accumulatorWindow) Expand(time.Time) {
	// No expand operation for Accumulator window
}

// windowState maintains the state of the window, consisting of the window and the message timestamps.
type windowState struct {
	window window.TimedWindow
	// messageTimestamps will track every inflight message's timestamp. It will be cleared
	// during a GC event (once WM progresses).
	messageTimestamps []time.Time
	// FIXME: still not required?
	// lastSeenEventTime is to see what was latest we have event time ever seen. This cannot be
	// messageTimestamps[-1] because that list will be cleared due to timeout. Since WM is global (across keys)
	// we need to trigger a close window to the UDF once the timeout has passed (WM > last-seen + timeout).
	lastSeenEventTime time.Time
}

func newWindowState(window window.TimedWindow) *windowState {
	return &windowState{
		window:            window,
		messageTimestamps: []time.Time{},
		lastSeenEventTime: time.UnixMilli(-1),
	}
}

// appendToTimestampList adds the event time to the inflight messages timestamp list in a sorted order.
func (ws *windowState) appendToTimestampList(eventTime time.Time) {
	// Find the insertion point using binary search
	index := sort.Search(len(ws.messageTimestamps), func(i int) bool {
		return ws.messageTimestamps[i].After(eventTime)
	})
	// Insert the event time at the found index
	ws.messageTimestamps = append(ws.messageTimestamps, time.Time{})
	copy(ws.messageTimestamps[index+1:], ws.messageTimestamps[index:])
	ws.messageTimestamps[index] = eventTime

	// Update the latest event time
	if eventTime.After(ws.lastSeenEventTime) {
		ws.lastSeenEventTime = eventTime
	}
}

// deleteEventTimesBefore deletes the event times from the window state before the given end time.
func (ws *windowState) deleteEventTimesBefore(endTime time.Time) {
	// Find the first index where the event time is after the end time using binary search
	index := sort.Search(len(ws.messageTimestamps), func(i int) bool {
		return ws.messageTimestamps[i].After(endTime)
	})
	// Keep only the event times after the end time
	ws.messageTimestamps = ws.messageTimestamps[index:]

	// Update the latest event time
	if len(ws.messageTimestamps) > 0 {
		ws.lastSeenEventTime = ws.messageTimestamps[len(ws.messageTimestamps)-1]
	} else {
		ws.lastSeenEventTime = time.Unix(0, 0)
	}
}

// Windower is an implementation of window.TimedWindower for Accumulator window.
type Windower struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	timeout       time.Duration
	// activeWindows captures the windowState for every key.
	activeWindows map[string]*windowState
}

// NewWindower creates a new Windower for Accumulator window.
func NewWindower(vertexInstance *dfv1.VertexInstance, timeout time.Duration) window.TimedWindower {
	return &Windower{
		timeout:       timeout,
		vertexName:    vertexInstance.Vertex.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		activeWindows: make(map[string]*windowState),
	}
}

var _ window.TimedWindower = (*Windower)(nil)

func (w *Windower) Strategy() window.Strategy {
	return window.Accumulator
}

// Type returns the window type. Accumulator window falls under the unaligned window type since it doesn't have
// a fixed window size.
func (w *Windower) Type() window.Type {
	return window.Unaligned
}

// AssignWindows assigns the windows for the given message. Since accumulator is based on the global window concept,
// we will have only one window per key.
func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	combinedKey := strings.Join(message.Keys, dfv1.KeysDelimitter)
	var op = window.Append

	ws, ok := w.activeWindows[combinedKey]
	// if we are seeing the key for the first time or if we are seeing the key after the timeout has expired create a
	// new window.
	if !ok {
		win := NewAccumulatorWindow(message.Keys)
		ws = newWindowState(win)
		w.activeWindows[combinedKey] = ws
		// this is a new window
		op = window.Open
	}

	// track the event times of the messages, will be used for publishing idle watermarks
	ws.appendToTimestampList(message.EventTime)

	return []*window.TimedWindowRequest{
		{
			ReadMessage: message,
			Operation:   op,
			Windows:     []window.TimedWindow{ws.window},
			ID:          ws.window.Partition(),
		},
	}
}

// InsertWindow inserts the window into the active windows. Since we have only one window for the key, it replaces
// the existing window.
func (w *Windower) InsertWindow(_ window.TimedWindow) {
	// Not Applicable for UnAligned
}

// CloseWindows closes the windows that have expired based on the timeout and since the WM has progressed.
func (w *Windower) CloseWindows(currentTime time.Time) []*window.TimedWindowRequest {
	var requests []*window.TimedWindowRequest
	for key, ws := range w.activeWindows {
		// check the latest event time we have seen for the key, if it's older than the timeout, close the window.
		if currentTime.After(ws.lastSeenEventTime.Add(w.timeout)) {
			requests = append(requests, &window.TimedWindowRequest{
				Operation: window.Close,
				Windows:   []window.TimedWindow{ws.window},
				ID:        ws.window.Partition(),
			})
			delete(w.activeWindows, key)
		}
	}
	return requests
}

// NextWindowToBeClosed returns the next window to be closed. Since accumulator is based on the global window concept,
// we don't have a window to be closed.
func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	// No windows get closed in accumulator
	return nil
}

// DeleteClosedWindow deletes the event time entries from the window state till the window end time.
// Actual deletion of the state happens when the timeout expires.
func (w *Windower) DeleteClosedWindow(window window.TimedWindow) {
	// delete event times less than window end time
	if ws, ok := w.activeWindows[strings.Join(window.Keys(), dfv1.KeysDelimitter)]; ok {
		ws.deleteEventTimesBefore(window.EndTime())
	}
}

// OldestWindowEndTime returns the oldest event time among all the keyed windows.
func (w *Windower) OldestWindowEndTime() time.Time {
	var minEndTime = time.UnixMilli(-1)
	for _, ws := range w.activeWindows {
		if len(ws.messageTimestamps) > 0 && (minEndTime.UnixMilli() == -1 || ws.messageTimestamps[0].Before(minEndTime)) {
			minEndTime = ws.messageTimestamps[0]
		}
	}
	return minEndTime
}

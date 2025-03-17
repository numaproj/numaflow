package accumulate

import (
	"fmt"
	"math"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

// accumulatorWindow TimedWindow implementation for Accumulator window.
type accumulatorWindow struct {
	startTime time.Time
	endTime   time.Time
	slot      string
	keys      []string
	partition *partition.ID
	id        string
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
		id:        fmt.Sprintf("%d-%d-%s-%s", 0, math.MaxInt64, slot, strings.Join(keys, dfv1.KeysDelimitter)),
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
	return w.id
}

func (w *accumulatorWindow) Merge(window.TimedWindow) {
	// No merge operation for Accumulator window
}

func (w *accumulatorWindow) Expand(time.Time) {
	// No expand operation for Accumulator window
}

// windowState maintains the state of the window, consisting of the window and the message timestamps.
type windowState struct {
	window            window.TimedWindow
	messageTimestamps []time.Time
	latestEventTime   time.Time
}

func newWindowState(window window.TimedWindow) *windowState {
	return &windowState{
		window:            window,
		messageTimestamps: []time.Time{},
		latestEventTime:   time.UnixMilli(-1),
	}
}

func (ws *windowState) addEventTime(eventTime time.Time) {
	ws.messageTimestamps = append(ws.messageTimestamps, eventTime)
	if eventTime.After(ws.latestEventTime) {
		ws.latestEventTime = eventTime
	}
}

func (ws *windowState) deleteEventTimesBefore(endTime time.Time) {
	var newEventTimes []time.Time
	for _, et := range ws.messageTimestamps {
		if et.After(endTime) {
			newEventTimes = append(newEventTimes, et)
		}
	}
	ws.messageTimestamps = newEventTimes
	if len(ws.messageTimestamps) > 0 {
		ws.latestEventTime = ws.messageTimestamps[len(ws.messageTimestamps)-1]
	} else {
		ws.latestEventTime = time.Unix(0, 0)
	}
}

type Windower struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	ttl           time.Duration
	activeWindows map[string]*windowState
}

func NewWindower(vertexInstance *dfv1.VertexInstance, ttl time.Duration) window.TimedWindower {
	return &Windower{
		ttl:           ttl,
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

func (w *Windower) Type() window.Type {
	return window.Unaligned
}

func (w *Windower) AssignWindows(message *isb.ReadMessage) []*window.TimedWindowRequest {
	combinedKey := strings.Join(message.Keys, dfv1.KeysDelimitter)
	ws, ok := w.activeWindows[combinedKey]
	if !ok {
		win := NewAccumulatorWindow(message.Keys)
		ws = newWindowState(win)
		w.activeWindows[combinedKey] = ws
	}
	ws.addEventTime(message.EventTime)
	return []*window.TimedWindowRequest{
		{
			ReadMessage: message,
			Operation:   window.Append,
			Windows:     []window.TimedWindow{ws.window},
			ID:          ws.window.Partition(),
		},
	}
}

func (w *Windower) InsertWindow(tw window.TimedWindow) {
	combinedKey := strings.Join(tw.Keys(), dfv1.KeysDelimitter)
	win := tw.(*accumulatorWindow)
	ws := newWindowState(win)
	w.activeWindows[combinedKey] = ws
}

func (w *Windower) CloseWindows(currentTime time.Time) []*window.TimedWindowRequest {
	var requests []*window.TimedWindowRequest
	for key, ws := range w.activeWindows {
		if currentTime.After(ws.latestEventTime.Add(w.ttl)) {
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

func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	// No windows get closed in accumulator
	return nil
}

func (w *Windower) DeleteClosedWindow(window window.TimedWindow) {
	// delete event times less than window end time
	if ws, ok := w.activeWindows[strings.Join(window.Keys(), dfv1.KeysDelimitter)]; ok {
		ws.deleteEventTimesBefore(window.EndTime())
	}
}

func (w *Windower) OldestWindowEndTime() time.Time {
	var minEndTime = time.UnixMilli(-1)
	for _, ws := range w.activeWindows {
		if len(ws.messageTimestamps) > 0 && (minEndTime.UnixMilli() == -1 || ws.messageTimestamps[0].Before(minEndTime)) {
			minEndTime = ws.messageTimestamps[0]
		}
	}
	return minEndTime
}

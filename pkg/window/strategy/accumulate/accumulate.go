package accumulate

import (
	"fmt"
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

func NewAccumulatorWindow(eventTime time.Time, keys []string) window.TimedWindow {
	slot := "slot-0"
	return &accumulatorWindow{
		startTime: eventTime,
		endTime:   eventTime,
		slot:      slot,
		keys:      keys,
		partition: &window.SharedUnalignedPartition,
		id:        fmt.Sprintf("%d-%d-%s-%s", eventTime.UnixMilli(), eventTime.UnixMilli(), slot, strings.Join(keys, dfv1.KeysDelimitter)),
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

func (w *accumulatorWindow) Merge(tw window.TimedWindow) {
	// No merge operation for Accumulator window
}

func (w *accumulatorWindow) Expand(endTime time.Time) {
	// No expand operation for Accumulator window
}

// Windower is an implementation of TimedWindower for Accumulator window.
type Windower struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	activeWindows map[string]*window.SortedWindowListByEndTime
}

func NewWindower(vertexInstance *dfv1.VertexInstance) window.TimedWindower {
	return &Windower{
		vertexName:    vertexInstance.Vertex.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
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
	list, ok := w.activeWindows[combinedKey]
	operation := window.Append
	if !ok {
		list = window.NewSortedWindowListByEndTime()
		w.activeWindows[combinedKey] = list
		operation = window.Open
	}

	win := NewAccumulatorWindow(message.EventTime, message.Keys).(*accumulatorWindow)
	list.Insert(win)

	return []*window.TimedWindowRequest{
		{
			ReadMessage: message,
			Operation:   operation,
			Windows:     []window.TimedWindow{win},
			ID:          win.Partition(),
		},
	}
}

func (w *Windower) InsertWindow(tw window.TimedWindow) {
	combinedKey := strings.Join(tw.Keys(), dfv1.KeysDelimitter)
	list, ok := w.activeWindows[combinedKey]
	if !ok {
		list = window.NewSortedWindowListByEndTime()
		w.activeWindows[combinedKey] = list
	}
	list.Insert(tw)
}

func (w *Windower) CloseWindows(time time.Time) []*window.TimedWindowRequest {
	// consider ttl?
	return nil
}

func (w *Windower) NextWindowToBeClosed() window.TimedWindow {
	// no windows gets closed in accumulator
	return nil
}

func (w *Windower) DeleteClosedWindow(window window.TimedWindow) {
	combinedKey := strings.Join(window.Keys(), dfv1.KeysDelimitter)
	if list, ok := w.activeWindows[combinedKey]; ok {
		list.RemoveWindows(window.EndTime().Add(-1 * time.Millisecond))
		if list.Len() == 0 {
			delete(w.activeWindows, combinedKey)
		}
	}
}

func (w *Windower) OldestWindowEndTime() time.Time {
	var minEndTime = time.UnixMilli(-1)
	for _, list := range w.activeWindows {
		if win := list.Front(); win != nil && (minEndTime.UnixMilli() == -1 || win.EndTime().Before(minEndTime)) {
			minEndTime = win.EndTime()
		}
	}
	return minEndTime
}

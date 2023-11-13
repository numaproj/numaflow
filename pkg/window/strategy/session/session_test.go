package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestNewWindow(t *testing.T) {
	startTime := time.Now()
	gap := time.Minute
	message := &isb.ReadMessage{}

	win := NewWindow(startTime, gap, message)

	assert.Equal(t, startTime, win.StartTime())
	assert.Equal(t, startTime.Add(gap), win.EndTime())
	assert.Equal(t, "slot-0", win.Slot())
	assert.Equal(t, message.Keys, win.Keys())
}

func TestAssignWindows_NewWindow(t *testing.T) {
	baseTime := time.UnixMilli(60000)
	gap := time.Second * 10
	windower := NewWindower(gap)

	message := buildReadMessage(baseTime, []string{"key1"})
	windowOperations := windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Open, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 1, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime, windowOperations[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(gap), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, &GlobalPartition, windowOperations[0].ID)

	// 2nd message should be assigned to the same window, since key is same
	message = buildReadMessage(baseTime.Add(5*time.Second), []string{"key1"})
	windowOperations = windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Expand, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 2, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime, windowOperations[0].Windows[0].StartTime())
	// 0th index we should have the old window, and 1st index we should have the new window
	assert.Equal(t, baseTime.Add(gap), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, message.EventTime.Add(gap), windowOperations[0].Windows[1].EndTime())
	assert.Equal(t, &GlobalPartition, windowOperations[0].ID)

	// 3rd message should be assigned to a new window, since key is different
	message = buildReadMessage(baseTime, []string{"key2"})
	windowOperations = windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Open, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 1, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime, windowOperations[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(gap), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, &GlobalPartition, windowOperations[0].ID)

	// 4th message should be assigned to a new window, because of the gap duration
	message = buildReadMessage(baseTime.Add(20*time.Second), []string{"key2"})
	windowOperations = windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Open, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 1, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime.Add(20*time.Second), windowOperations[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(30*time.Second), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, &GlobalPartition, windowOperations[0].ID)
}

func TestSession_InsertWindow(t *testing.T) {
	win1 := &Window{
		startTime: time.UnixMilli(60000),
		endTime:   time.UnixMilli(60000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win2 := &Window{
		startTime: time.UnixMilli(60000),
		endTime:   time.UnixMilli(60000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower := &Windower{
		gap:           10 * time.Second,
		activeWindows: make(map[string]*window.SortedWindowListByStartTime[window.TimedWindow]),
	}

	windower.InsertWindow(win1)
	assert.Equal(t, 1, windower.activeWindows["key-1"].Len())

	windower.InsertWindow(win2)
	assert.Equal(t, 1, windower.activeWindows["key-2"].Len())

	win2 = &Window{
		startTime: time.UnixMilli(120000),
		endTime:   time.UnixMilli(120000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower.InsertWindow(win2)

	// since this is a different window, the active windows should be 2
	assert.Equal(t, 2, windower.activeWindows["key-2"].Len())
}

func buildReadMessage(time time.Time, keys []string) *isb.ReadMessage {
	return &isb.ReadMessage{
		Message: isb.Message{
			Header: isb.Header{
				Keys: keys,
				MessageInfo: isb.MessageInfo{
					EventTime: time,
				},
			},
		},
	}
}

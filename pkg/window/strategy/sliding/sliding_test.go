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
package sliding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestSliding_AssignWindowsLengthDivisibleBySlide(t *testing.T) {
	baseTime := time.UnixMilli(60000)
	windower := NewWindower(time.Minute, 20*time.Second)

	readMsg := buildReadMessage(baseTime.Add(10 * time.Second))
	windowRequests := windower.AssignWindows(readMsg)

	// since this is the first message, the window operation should be open
	assert.Equal(t, 3, len(windowRequests))
	assert.Equal(t, baseTime, windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: baseTime,
		End:   baseTime.Add(60 * time.Second),
		Slot:  "slot-0",
	}, windowRequests[0].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[0].Operation)

	assert.Equal(t, baseTime.Add(-20*time.Second), windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(40*time.Second), windowRequests[1].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: baseTime.Add(-20 * time.Second),
		End:   baseTime.Add(40 * time.Second),
		Slot:  "slot-0",
	}, windowRequests[1].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[1].Operation)

	assert.Equal(t, baseTime.Add(-40*time.Second), windowRequests[2].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(20*time.Second), windowRequests[2].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: baseTime.Add(-40 * time.Second),
		End:   baseTime.Add(20 * time.Second),
		Slot:  "slot-0",
	}, windowRequests[2].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[2].Operation)

	readMsg = buildReadMessage(baseTime.Add(1 * time.Second))
	windowRequests = windower.AssignWindows(readMsg)

	assert.Equal(t, 3, len(windowRequests))
	assert.Equal(t, window.Append, windowRequests[0].Operation)
	assert.Equal(t, window.Append, windowRequests[1].Operation)
	assert.Equal(t, window.Append, windowRequests[2].Operation)
}

// TODO add more tests to cover AssignWindow edge cases
func TestSliding_AssignWindowsLengthNotDivisibleBySlide(t *testing.T) {
	// length 60s, slide 40s
	baseTime := time.Unix(600, 0)
	windower := NewWindower(time.Minute, 40*time.Second)

	readMsg := buildReadMessage(baseTime.Add(10 * time.Second))
	windowRequests := windower.AssignWindows(readMsg)

	// since this is the first message, the window operation should be open
	assert.Equal(t, 2, len(windowRequests))
	assert.Equal(t, time.Unix(600, 0), windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, time.Unix(660, 0), windowRequests[0].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: time.Unix(600, 0),
		End:   time.Unix(660, 0),
		Slot:  "slot-0",
	}, windowRequests[0].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[0].Operation)

	assert.Equal(t, time.Unix(560, 0), windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, time.Unix(620, 0), windowRequests[1].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: time.Unix(560, 0),
		End:   time.Unix(620, 0),
		Slot:  "slot-0",
	}, windowRequests[1].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[1].Operation)

	readMsg = buildReadMessage(baseTime.Add(1 * time.Second))
	windowRequests = windower.AssignWindows(readMsg)

	assert.Equal(t, 2, len(windowRequests))
	assert.Equal(t, window.Append, windowRequests[0].Operation)
	assert.Equal(t, window.Append, windowRequests[1].Operation)
}

func TestSliding_InsertWindow(t *testing.T) {
	win := &slidingWindow{
		startTime: time.UnixMilli(60000),
		endTime:   time.UnixMilli(60000 + 60*1000),
		slot:      "slot-0",
	}

	windower := &Windower{
		length:        60 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
	}

	windower.InsertWindow(win)

	// since this is the first time the window is inserted, the active windows should be 1
	assert.Equal(t, 1, windower.activeWindows.Len())

	windower.InsertWindow(win)

	// since this is the second time we are inserting the same window, the active windows should be 1
	assert.Equal(t, 1, windower.activeWindows.Len())

	win = &slidingWindow{
		startTime: time.UnixMilli(120000),
		endTime:   time.UnixMilli(120000 + 60*1000),
		slot:      "slot-0",
	}

	windower.InsertWindow(win)

	// since this is a different window, the active windows should be 2
	assert.Equal(t, 2, windower.activeWindows.Len())
}

func TestSliding_CloseWindows(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &slidingWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
	}
	win2 := &slidingWindow{
		startTime: baseTime.Add(-10 * time.Second),
		endTime:   baseTime.Add(50 * time.Second),
	}
	win3 := &slidingWindow{
		startTime: baseTime.Add(-20 * time.Second),
		endTime:   baseTime.Add(40 * time.Second),
	}

	windower := NewWindower(60*time.Second, 10*time.Second)

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close the window with end time less than baseTime + 120 seconds
	windowRequests := windower.CloseWindows(baseTime.Add(120 * time.Second))

	assert.Equal(t, 3, len(windowRequests))

	assert.Equal(t, window.Delete, windowRequests[0].Operation)
	assert.Equal(t, baseTime.Add(-20*time.Second), windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(40*time.Second), windowRequests[0].Windows[0].EndTime())

	assert.Equal(t, window.Delete, windowRequests[1].Operation)
	assert.Equal(t, baseTime.Add(-10*time.Second), windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(50*time.Second), windowRequests[1].Windows[0].EndTime())

	assert.Equal(t, window.Delete, windowRequests[2].Operation)
	assert.Equal(t, baseTime, windowRequests[2].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[2].Windows[0].EndTime())

}

func TestSliding_DeleteWindows(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &slidingWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
	}
	win2 := &slidingWindow{
		startTime: baseTime.Add(-10 * time.Second),
		endTime:   baseTime.Add(50 * time.Second),
	}
	win3 := &slidingWindow{
		startTime: baseTime.Add(-20 * time.Second),
		endTime:   baseTime.Add(40 * time.Second),
	}

	windower := &Windower{
		length:        60 * time.Second,
		slide:         10 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close all the windows
	windower.CloseWindows(baseTime.Add(120 * time.Second))

	// delete one of the windows
	windower.DeleteClosedWindows(&window.TimedWindowResponse{
		Window: window.NewWindowFromPartition(&partition.ID{
			Start: baseTime,
			End:   baseTime.Add(60 * time.Second),
			Slot:  "slot-0",
		}),
	})

	// since we deleted one of the windows, the closed windows should be 2
	assert.Equal(t, 2, windower.closedWindows.Len())
}

func TestSliding_OldestClosedWindowEndTime(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &slidingWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
	}
	win2 := &slidingWindow{
		startTime: baseTime.Add(-10 * time.Second),
		endTime:   baseTime.Add(50 * time.Second),
	}
	win3 := &slidingWindow{
		startTime: baseTime.Add(-20 * time.Second),
		endTime:   baseTime.Add(40 * time.Second),
	}

	windower := &Windower{
		length:        60 * time.Second,
		slide:         10 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close all the windows
	windower.CloseWindows(baseTime.Add(180 * time.Second))

	// oldest closed window is (60000, 120000)
	assert.Equal(t, baseTime.Add(40*time.Second), windower.OldestWindowEndTime())
}

func TestSliding_NextWindowToBeClosed(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &slidingWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
	}
	win2 := &slidingWindow{
		startTime: baseTime.Add(-10 * time.Second),
		endTime:   baseTime.Add(50 * time.Second),
	}
	win3 := &slidingWindow{
		startTime: baseTime.Add(-20 * time.Second),
		endTime:   baseTime.Add(40 * time.Second),
	}

	windower := &Windower{
		length:        60 * time.Second,
		slide:         10 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// next window to be closed is (60000, 120000)
	assert.Equal(t, baseTime.Add(-20*time.Second), windower.NextWindowToBeClosed().StartTime())
	assert.Equal(t, baseTime.Add(40*time.Second), windower.NextWindowToBeClosed().EndTime())
}

func buildReadMessage(time time.Time) *isb.ReadMessage {
	return &isb.ReadMessage{
		Message: isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: time,
				},
			},
		},
	}
}

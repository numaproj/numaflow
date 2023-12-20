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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestNewWindow(t *testing.T) {
	startTime := time.Now()
	gap := time.Minute
	message := &isb.ReadMessage{}

	win := NewSessionWindow(startTime, gap, message)

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
	assert.Equal(t, &SharedSessionPartition, windowOperations[0].ID)

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
	assert.Equal(t, &SharedSessionPartition, windowOperations[0].ID)

	// 3rd message should be assigned to a new window, since key is different
	message = buildReadMessage(baseTime, []string{"key2"})
	windowOperations = windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Open, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 1, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime, windowOperations[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(gap), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, &SharedSessionPartition, windowOperations[0].ID)

	// 4th message should be assigned to a new window, because of the gap duration
	message = buildReadMessage(baseTime.Add(20*time.Second), []string{"key2"})
	windowOperations = windower.AssignWindows(message)

	assert.Len(t, windowOperations, 1)
	assert.Equal(t, window.Open, windowOperations[0].Operation)
	assert.Equal(t, message, windowOperations[0].ReadMessage)
	assert.Equal(t, 1, len(windowOperations[0].Windows))
	assert.Equal(t, baseTime.Add(20*time.Second), windowOperations[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(30*time.Second), windowOperations[0].Windows[0].EndTime())
	assert.Equal(t, &SharedSessionPartition, windowOperations[0].ID)
}

func TestSession_InsertWindow(t *testing.T) {
	win1 := &sessionWindow{
		startTime: time.UnixMilli(60000),
		endTime:   time.UnixMilli(60000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win2 := &sessionWindow{
		startTime: time.UnixMilli(60000),
		endTime:   time.UnixMilli(60000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower := &Windower{
		gap:           10 * time.Second,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
	}

	windower.InsertWindow(win1)
	assert.Equal(t, 1, windower.activeWindows["key-1"].Len())

	windower.InsertWindow(win2)
	assert.Equal(t, 1, windower.activeWindows["key-2"].Len())

	win2 = &sessionWindow{
		startTime: time.UnixMilli(120000),
		endTime:   time.UnixMilli(120000 + 60*1000),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower.InsertWindow(win2)

	// since this is a different window, the active windows should be 2
	assert.Equal(t, 2, windower.activeWindows["key-2"].Len())
}

func TestSession_CloseWindowsWithoutMerge(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &sessionWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(10 * time.Second),
		slot:      "slot-0",
	}
	win2 := &sessionWindow{
		startTime: baseTime.Add(30 * time.Second),
		endTime:   baseTime.Add(59 * time.Second),
	}
	win3 := &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(90 * time.Second),
	}

	windower := NewWindower(10 * time.Second)

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close the window with end time less than baseTime + 120 seconds
	windowRequests := windower.CloseWindows(baseTime.Add(60 * time.Second))

	assert.Equal(t, 2, len(windowRequests))

	assert.Equal(t, window.Close, windowRequests[0].Operation)
	assert.Equal(t, baseTime.Add(30*time.Second), windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(59*time.Second), windowRequests[0].Windows[0].EndTime())

	assert.Equal(t, window.Close, windowRequests[1].Operation)
	assert.Equal(t, baseTime, windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(10*time.Second), windowRequests[1].Windows[0].EndTime())

	windowRequests = windower.CloseWindows(baseTime.Add(120 * time.Second))
	assert.Equal(t, 1, len(windowRequests))

	assert.Equal(t, window.Close, windowRequests[0].Operation)
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(90*time.Second), windowRequests[0].Windows[0].EndTime())
}

func TestSession_CloseWindowsWithMerge(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	// win1 and win2 should be merged
	win1 := &sessionWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(20 * time.Second),
		slot:      "slot-0",
	}
	win2 := &sessionWindow{
		startTime: baseTime.Add(11 * time.Second),
		endTime:   baseTime.Add(59 * time.Second),
		slot:      "slot-0",
	}
	win3 := &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(90 * time.Second),
		slot:      "slot-0",
	}

	windower := NewWindower(10 * time.Second)

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close the window with end time less than baseTime + 120 seconds
	windowRequests := windower.CloseWindows(baseTime.Add(120 * time.Second))
	assert.Equal(t, 3, len(windowRequests))

	// close operation on the window which was not merged
	assert.Equal(t, window.Close, windowRequests[0].Operation)
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(90*time.Second), windowRequests[0].Windows[0].EndTime())

	// merge operation should be performed first
	assert.Equal(t, window.Merge, windowRequests[1].Operation)
	assert.Equal(t, 2, len(windowRequests[1].Windows))

	assert.Equal(t, baseTime, windowRequests[1].Windows[1].StartTime())
	assert.Equal(t, baseTime.Add(20*time.Second), windowRequests[1].Windows[1].EndTime())
	assert.Equal(t, baseTime.Add(11*time.Second), windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(59*time.Second), windowRequests[1].Windows[0].EndTime())

	// close operation on the merged window
	assert.Equal(t, window.Close, windowRequests[2].Operation)
	assert.Equal(t, baseTime, windowRequests[2].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(59*time.Second), windowRequests[2].Windows[0].EndTime())

	// test windows with different keys
	win1 = &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(89 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win2 = &sessionWindow{
		startTime: baseTime.Add(50 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win3 = &sessionWindow{
		startTime: baseTime.Add(30 * time.Second),
		endTime:   baseTime.Add(150 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win4 := &sessionWindow{
		startTime: baseTime.Add(30 * time.Second),
		endTime:   baseTime.Add(50 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}
	win5 := &sessionWindow{
		startTime: baseTime.Add(40 * time.Second),
		endTime:   baseTime.Add(80 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}
	win6 := &sessionWindow{
		startTime: baseTime.Add(10 * time.Second),
		endTime:   baseTime.Add(90 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)
	windower.InsertWindow(win4)
	windower.InsertWindow(win5)
	windower.InsertWindow(win6)

	// close the window with end time less than baseTime + 150 seconds
	windowRequests = windower.CloseWindows(baseTime.Add(150 * time.Second))
	assert.Equal(t, 4, len(windowRequests))
	// merge operation for key-1
	assert.Equal(t, window.Merge, windowRequests[0].Operation)
	// all three windows should be merged
	assert.Equal(t, 3, len(windowRequests[0].Windows))
	// close operation for merged window key-1
	assert.Equal(t, window.Close, windowRequests[1].Operation)
	// merge operation for key-2
	assert.Equal(t, window.Merge, windowRequests[2].Operation)
	// all three windows should be merged
	assert.Equal(t, 3, len(windowRequests[2].Windows))
	// close operation for merged window key-2
	assert.Equal(t, window.Close, windowRequests[3].Operation)
}

func TestSession_CloseWindowsComplex(t *testing.T) {

	//	 window -  60000-119000-slot-0
	//	 window -  159000-218000-slot-0
	//	 window -  258000-279000-slot-0
	//	 window -  262000-317000-slot-0
	//	 window -  357000-416000-slot-0
	//	 window -  456000-515000-slot-0
	// 	 window -  514000-573000-slot-0

	win1 := &sessionWindow{
		startTime: time.Unix(60, 0),
		endTime:   time.Unix(119, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win2 := &sessionWindow{
		startTime: time.Unix(159, 0),
		endTime:   time.Unix(218, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win3 := &sessionWindow{
		startTime: time.Unix(258, 0),
		endTime:   time.Unix(279, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win4 := &sessionWindow{
		startTime: time.Unix(262, 0),
		endTime:   time.Unix(317, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win5 := &sessionWindow{
		startTime: time.Unix(357, 0),
		endTime:   time.Unix(416, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win6 := &sessionWindow{
		startTime: time.Unix(456, 0),
		endTime:   time.Unix(515, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	win7 := &sessionWindow{
		startTime: time.Unix(514, 0),
		endTime:   time.Unix(573, 0),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	windower := &Windower{
		gap:           10 * time.Second,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)
	windower.InsertWindow(win4)
	windower.InsertWindow(win5)
	windower.InsertWindow(win6)
	windower.InsertWindow(win7)

	closeWindowOps := windower.CloseWindows(time.Unix(515, 0))

	//Merge
	//456000-515000-slot-0
	//514000-573000-slot-0

	//Close
	//357000-416000-slot-0

	//Merge
	//262000-317000-slot-0
	//258000-279000-slot-0

	//Close
	//258000-317000-slot-0

	//Close
	//159000-218000-slot-0

	//Close
	//60000-119000-slot-0

	assert.Equal(t, 6, len(closeWindowOps))
	assert.Equal(t, window.Merge, closeWindowOps[0].Operation)
	assert.Equal(t, int64(456000), closeWindowOps[0].Windows[0].StartTime().UnixMilli())
	assert.Equal(t, int64(515000), closeWindowOps[0].Windows[0].EndTime().UnixMilli())
	assert.Equal(t, int64(514000), closeWindowOps[0].Windows[1].StartTime().UnixMilli())
	assert.Equal(t, int64(573000), closeWindowOps[0].Windows[1].EndTime().UnixMilli())

	assert.Equal(t, window.Close, closeWindowOps[1].Operation)
	assert.Equal(t, int64(357000), closeWindowOps[1].Windows[0].StartTime().UnixMilli())
	assert.Equal(t, int64(416000), closeWindowOps[1].Windows[0].EndTime().UnixMilli())

	assert.Equal(t, window.Merge, closeWindowOps[2].Operation)
	assert.Equal(t, int64(262000), closeWindowOps[2].Windows[0].StartTime().UnixMilli())
	assert.Equal(t, int64(317000), closeWindowOps[2].Windows[0].EndTime().UnixMilli())
	assert.Equal(t, int64(258000), closeWindowOps[2].Windows[1].StartTime().UnixMilli())
	assert.Equal(t, int64(279000), closeWindowOps[2].Windows[1].EndTime().UnixMilli())

	assert.Equal(t, window.Close, closeWindowOps[3].Operation)
	assert.Equal(t, int64(258000), closeWindowOps[3].Windows[0].StartTime().UnixMilli())

	assert.Equal(t, window.Close, closeWindowOps[4].Operation)
	assert.Equal(t, int64(159000), closeWindowOps[4].Windows[0].StartTime().UnixMilli())

	assert.Equal(t, window.Close, closeWindowOps[5].Operation)
	assert.Equal(t, int64(60000), closeWindowOps[5].Windows[0].StartTime().UnixMilli())

}

func TestSession_DeleteWindows(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &sessionWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win2 := &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win3 := &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}
	win4 := &sessionWindow{
		startTime: baseTime.Add(120 * time.Second),
		endTime:   baseTime.Add(180 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower := &Windower{
		gap:           10 * time.Second,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)
	windower.InsertWindow(win4)

	// close all the windows
	windower.CloseWindows(baseTime.Add(180 * time.Second))

	// delete one of the windows
	windower.DeleteClosedWindow(&window.TimedWindowResponse{
		Window: window.NewWindowFromPartitionAndKeys(&partition.ID{
			Start: baseTime,
			End:   baseTime.Add(60 * time.Second),
			Slot:  "slot-0",
		}, []string{"key-1"}),
	})

	// since we deleted one of the windows, the closed windows should be 3
	assert.Equal(t, 3, windower.closedWindows.Len())

	windower.DeleteClosedWindow(&window.TimedWindowResponse{
		Window: window.NewWindowFromPartitionAndKeys(&partition.ID{
			Start: baseTime.Add(60 * time.Second),
			End:   baseTime.Add(120 * time.Second),
			Slot:  "slot-0",
		}, []string{"key-1"}),
	})
	// since we deleted two windows, the closed windows should be 2
	assert.Equal(t, 2, windower.closedWindows.Len())
}

func TestWindower_OldestClosedWindowEndTime(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &sessionWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win2 := &sessionWindow{
		startTime: baseTime.Add(40 * time.Second),
		endTime:   baseTime.Add(70 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win3 := &sessionWindow{
		startTime: baseTime.Add(10 * time.Second),
		endTime:   baseTime.Add(90 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-2"},
	}

	windower := &Windower{
		gap:           10 * time.Second,
		activeWindows: make(map[string]*window.SortedWindowListByEndTime),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win2)
	windower.InsertWindow(win1)
	windower.InsertWindow(win3)

	// close all the windows
	windower.CloseWindows(baseTime.Add(180 * time.Second))

	// when we close the windows window (60, 120) and (100, 130) will be merged
	// so the oldest window end time will be 130
	assert.Equal(t, baseTime.Add(70*time.Second), windower.OldestWindowEndTime())

	// delete one of the windows
	windower.DeleteClosedWindow(&window.TimedWindowResponse{
		Window: window.NewWindowFromPartitionAndKeys(&partition.ID{
			Start: baseTime,
			End:   baseTime.Add(70 * time.Second),
			Slot:  "slot-0",
		}, []string{"key-1"}),
	})

	// since we deleted (60, 130) window, now the oldest window end time will be 90
	assert.Equal(t, baseTime.Add(90*time.Second), windower.OldestWindowEndTime())
}

func TestWindower_NextWindowToBeClosed(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &sessionWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(10 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win2 := &sessionWindow{
		startTime: baseTime.Add(30 * time.Second),
		endTime:   baseTime.Add(59 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}
	win3 := &sessionWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(90 * time.Second),
		slot:      "slot-0",
		keys:      []string{"key-1"},
	}

	windower := NewWindower(10 * time.Second)

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	assert.Equal(t, time.UnixMilli(0), windower.NextWindowToBeClosed().StartTime())
	assert.Equal(t, time.UnixMilli(math.MaxInt64), windower.NextWindowToBeClosed().EndTime())
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

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
package fixed

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

var keyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "test-pl",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			UDF:  &dfv1.UDF{GroupBy: &dfv1.GroupBy{Keyed: true}},
		},
	}},
	Hostname: "test-host",
	Replica:  0,
}

func TestFixed_AssignWindow(t *testing.T) {
	baseTime := time.UnixMilli(60000)
	windower := NewWindower(60*time.Second, keyedVertex)

	readMsg := buildReadMessage(baseTime)
	windowRequests := windower.AssignWindows(readMsg)

	// since this is the first message, the window operation should be open
	// and the window should be from 60000 to 60000 + 60 seconds
	assert.Equal(t, 1, len(windowRequests))
	assert.Equal(t, baseTime, windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: baseTime,
		End:   baseTime.Add(60 * time.Second),
		Slot:  "slot-0",
	}, windowRequests[0].Windows[0].Partition())
	assert.Equal(t, window.Open, windowRequests[0].Operation)

	readMsg = buildReadMessage(baseTime.Add(1 * time.Second))
	windowRequests = windower.AssignWindows(readMsg)

	// since this is the second message, the window operation should be append
	// and the window should be from 60000 to 60000 + 60 seconds
	assert.Equal(t, 1, len(windowRequests))
	assert.Equal(t, baseTime, windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].EndTime())
	assert.Equal(t, &partition.ID{
		Start: baseTime,
		End:   baseTime.Add(60 * time.Second),
		Slot:  "slot-0",
	}, windowRequests[0].Windows[0].Partition())
	assert.Equal(t, window.Append, windowRequests[0].Operation)
}

func TestFixed_InsertWindow(t *testing.T) {
	win := &fixedWindow{
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

	win = &fixedWindow{
		startTime: time.UnixMilli(120000),
		endTime:   time.UnixMilli(120000 + 60*1000),
		slot:      "slot-0",
	}

	windower.InsertWindow(win)

	// since this is a different window, the active windows should be 2
	assert.Equal(t, 2, windower.activeWindows.Len())
}

func TestFixed_CloseWindows(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &fixedWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
	}
	win2 := &fixedWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
	}
	win3 := &fixedWindow{
		startTime: baseTime.Add(120 * time.Second),
		endTime:   baseTime.Add(180 * time.Second),
	}

	windower := NewWindower(60*time.Second, keyedVertex)

	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close the window with end time less than baseTime + 120 seconds
	windowRequests := windower.CloseWindows(baseTime.Add(120 * time.Second))

	assert.Equal(t, 2, len(windowRequests))

	assert.Equal(t, window.Delete, windowRequests[0].Operation)
	assert.Equal(t, baseTime, windowRequests[0].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[0].Windows[0].EndTime())

	assert.Equal(t, window.Delete, windowRequests[1].Operation)
	assert.Equal(t, baseTime.Add(60*time.Second), windowRequests[1].Windows[0].StartTime())
	assert.Equal(t, baseTime.Add(120*time.Second), windowRequests[1].Windows[0].EndTime())

}

func TestFixed_DeleteWindows(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &fixedWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime,
			End:   baseTime.Add(60 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.UnixMilli(), baseTime.Add(60*time.Second).UnixMilli(), "slot-0"),
	}
	win2 := &fixedWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(60 * time.Second),
			End:   baseTime.Add(120 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(60*time.Second).UnixMilli(), baseTime.Add(120*time.Second).UnixMilli(), "slot-0"),
	}
	win3 := &fixedWindow{
		startTime: baseTime.Add(120 * time.Second),
		endTime:   baseTime.Add(180 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(120 * time.Second),
			End:   baseTime.Add(180 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(120*time.Second).UnixMilli(), baseTime.Add(180*time.Second).UnixMilli(), "slot-0"),
	}

	windower := &Windower{
		length:        60 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// close all the windows
	windower.CloseWindows(baseTime.Add(180 * time.Second))

	// delete one of the windows
	windower.DeleteClosedWindow(&fixedWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(60 * time.Second),
			End:   baseTime.Add(120 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(60*time.Second).UnixMilli(), baseTime.Add(120*time.Second).UnixMilli(), "slot-0"),
	})

	// since we deleted one of the windows, the closed windows should be 2
	assert.Equal(t, 2, windower.closedWindows.Len())
}

func TestFixed_OldestClosedWindowEndTime(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &fixedWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime,
			End:   baseTime.Add(60 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.UnixMilli(), baseTime.Add(60*time.Second).UnixMilli(), "slot-0"),
	}
	win2 := &fixedWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(60 * time.Second),
			End:   baseTime.Add(120 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(60*time.Second).UnixMilli(), baseTime.Add(120*time.Second).UnixMilli(), "slot-0"),
	}
	win3 := &fixedWindow{
		startTime: baseTime.Add(120 * time.Second),
		endTime:   baseTime.Add(180 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(120 * time.Second),
			End:   baseTime.Add(180 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(120*time.Second).UnixMilli(), baseTime.Add(180*time.Second).UnixMilli(), "slot-0"),
	}

	windower := &Windower{
		length:        60 * time.Second,
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
	assert.Equal(t, baseTime.Add(60*time.Second), windower.OldestWindowEndTime())
}

func TestWindower_NextWindowToBeClosed(t *testing.T) {
	baseTime := time.UnixMilli(60000)

	win1 := &fixedWindow{
		startTime: baseTime,
		endTime:   baseTime.Add(60 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime,
			End:   baseTime.Add(60 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.UnixMilli(), baseTime.Add(60*time.Second).UnixMilli(), "slot-0"),
	}
	win2 := &fixedWindow{
		startTime: baseTime.Add(60 * time.Second),
		endTime:   baseTime.Add(120 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(60 * time.Second),
			End:   baseTime.Add(120 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(60*time.Second).UnixMilli(), baseTime.Add(120*time.Second).UnixMilli(), "slot-0"),
	}
	win3 := &fixedWindow{
		startTime: baseTime.Add(120 * time.Second),
		endTime:   baseTime.Add(180 * time.Second),
		slot:      "slot-0",
		partition: &partition.ID{
			Start: baseTime.Add(120 * time.Second),
			End:   baseTime.Add(180 * time.Second),
			Slot:  "slot-0",
		},
		id: fmt.Sprintf("%d-%d-%s", baseTime.Add(120*time.Second).UnixMilli(), baseTime.Add(180*time.Second).UnixMilli(), "slot-0"),
	}

	windower := &Windower{
		length:        60 * time.Second,
		activeWindows: window.NewSortedWindowListByEndTime(),
		closedWindows: window.NewSortedWindowListByEndTime(),
	}

	// insert the windows
	windower.InsertWindow(win1)
	windower.InsertWindow(win2)
	windower.InsertWindow(win3)

	// next window to be closed is (60000, 120000)
	assert.Equal(t, baseTime.Add(60*time.Second), windower.NextWindowToBeClosed().EndTime())
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

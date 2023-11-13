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

package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

type TestWindow struct {
	start time.Time
	end   time.Time
	slot  string
	keys  []string
}

func (t *TestWindow) StartTime() time.Time {
	return t.start
}

func (t *TestWindow) EndTime() time.Time {
	return t.end
}

func (t *TestWindow) Slot() string {
	return t.slot
}

func (t *TestWindow) Partition() *partition.ID {
	return &partition.ID{
		Start: t.start,
		End:   t.end,
		Slot:  t.slot,
	}
}

func (t *TestWindow) Merge(tw TimedWindow) {
	//TODO implement me
	panic("implement me")
}

func (t *TestWindow) Expand(et time.Time) {
	//TODO implement me
	panic("implement me")
}

func (t *TestWindow) Keys() []string {
	return t.keys
}

// TODO add tests to cover different slot values

func TestSortedWindowList_InsertIfNotPresent(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           *TestWindow
		expectedWindows []*TestWindow
		isPresent       bool
	}{
		{
			name:  "FirstWindow",
			given: []*TestWindow{},
			input: &TestWindow{
				start: time.Unix(0, 0),
				end:   time.Unix(60, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "late_window",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindow{
				start: time.Unix(60, 0),
				end:   time.Unix(120, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "early_window",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "insert_middle",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "already_present",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			isPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			ret, isPresent := windows.InsertIfNotPresent(tt.input)
			assert.Equal(t, tt.isPresent, isPresent)
			assert.Equal(t, tt.input.start, ret.StartTime())
			assert.Equal(t, tt.input.end, ret.EndTime())
			assert.Equal(t, len(tt.expectedWindows), windows.Len())
			nodes := windows.Items()
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowList_RemoveWindows(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           time.Time
		expectedWindows []*TestWindow
	}{
		{
			name: "remove_windows",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: time.Unix(180, 0),
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			removedWindows := windows.RemoveWindows(tt.input)
			assert.Equal(t, len(tt.expectedWindows), len(removedWindows))
			nodes := removedWindows
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowList_DeleteWindow(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           *TestWindow
		expectedWindows []*TestWindow
	}{
		{
			name: "delete_middle",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "delete_first",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(120, 0),
				end:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "delete_last",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			windows.Delete(tt.input)
			assert.Equal(t, len(tt.expectedWindows), windows.Len())
			nodes := windows.Items()
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowList_InsertFront(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           *TestWindow
		expectedWindows []*TestWindow
	}{
		{
			name:  "insert_empty",
			given: []*TestWindow{},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_greater",
			given: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(300, 0),
				end:   time.Unix(360, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			windows.InsertFront(tt.input)
			assert.Equal(t, len(tt.expectedWindows), windows.Len())
			nodes := windows.Items()
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowList_InsertBack(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           *TestWindow
		expectedWindows []*TestWindow
	}{
		{
			name:  "insert_empty",
			given: []*TestWindow{},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_smaller",
			given: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(120, 0),
				end:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			windows.InsertBack(tt.input)
			assert.Equal(t, len(tt.expectedWindows), windows.Len())
			nodes := windows.Items()
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowListByStartTime_Insert(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           *TestWindow
		expectedWindows []*TestWindow
		isPresent       bool
	}{
		{
			name:  "FirstWindow",
			given: []*TestWindow{},
			input: &TestWindow{
				start: time.Unix(0, 0),
				end:   time.Unix(60, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "late_window",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindow{
				start: time.Unix(60, 0),
				end:   time.Unix(120, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "early_window",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindow{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
					slot:  "slot-0",
				},
			},
			isPresent: false,
		},
		{
			name: "insert_middle",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "already_present",
			given: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(180, 0),
					end:   time.Unix(240, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			isPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			windows.Insert(tt.input)
			assert.Equal(t, len(tt.expectedWindows), windows.Len())
			nodes := windows.Items()
			i := 0
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
				i += 1
			}
		})
	}
}

func TestSortedWindowListByStartTime_FindWindowForTime(t *testing.T) {
	tests := []struct {
		name           string
		given          []*TestWindow
		input          time.Time
		expectedWindow *TestWindow
		isPresent      bool
	}{
		{
			name: "one_window",
			given: []*TestWindow{
				{
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					slot:  "slot-0",
				},
			},
			input: time.Unix(30, 0),
			expectedWindow: &TestWindow{
				start: time.Unix(0, 0),
				end:   time.Unix(60, 0),
				slot:  "slot-0",
			},
			isPresent: true,
		},
		{
			name: "right_exclusive",
			given: []*TestWindow{
				{
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input:          time.Unix(60, 0),
			expectedWindow: &TestWindow{},
			isPresent:      false,
		},
		{
			name: "left_inclusive",
			given: []*TestWindow{
				{
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					slot:  "slot-0",
				},
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
					slot:  "slot-0",
				},
			},
			input: time.Unix(60, 0),
			expectedWindow: &TestWindow{
				start: time.Unix(60, 0),
				end:   time.Unix(120, 0),
				slot:  "slot-0",
			},
			isPresent: true,
		},
		{
			name: "multiple_present",
			given: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
				},
				{
					start: time.Unix(65, 0),
					end:   time.Unix(85, 0),
				},
				{
					start: time.Unix(70, 0),
					end:   time.Unix(130, 0),
				},
			},
			input: time.Unix(60, 0),
			expectedWindow: &TestWindow{
				start: time.Unix(60, 0),
				end:   time.Unix(120, 0),
			},
			isPresent: true,
		},
		{
			name: "last_present",
			given: []*TestWindow{
				{
					start: time.Unix(68, 0),
					end:   time.Unix(80, 0),
				},
				{
					start: time.Unix(77, 0),
					end:   time.Unix(79, 0),
				},
				{
					start: time.Unix(89, 0),
					end:   time.Unix(99, 0),
				},
			},
			input: time.Unix(90, 0),
			expectedWindow: &TestWindow{
				start: time.Unix(89, 0),
				end:   time.Unix(99, 0),
			},
			isPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByStartTime[*TestWindow]()
			setup(windows, tt.given)
			win, isPresent := windows.FindWindowForTime(tt.input)
			assert.Equal(t, tt.isPresent, isPresent)
			assert.Equal(t, win, tt.expectedWindow)
		})
	}
}

func setup(windows *SortedWindowListByStartTime[*TestWindow], wins []*TestWindow) {
	for _, win := range wins {
		windows.Insert(win)
	}
}

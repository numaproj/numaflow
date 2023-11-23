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

func TestSortedWindowListByEndTime_InsertIfNotPresent(t *testing.T) {
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
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_RemoveWindows(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindow
		input           time.Time
		expectedWindows []*TestWindow
	}{
		{
			name: "remove_first",
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
		{
			name: "remove_middle",
			given: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
				},
				{
					start: time.Unix(50, 0),
					end:   time.Unix(135, 0),
				},
				{
					start: time.Unix(100, 0),
					end:   time.Unix(145, 0),
				},
			},
			input: time.Unix(135, 0),
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(120, 0),
				},
				{
					start: time.Unix(50, 0),
					end:   time.Unix(135, 0),
				},
			},
		},
		{
			name: "remove_all",
			given: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(65, 0),
				},
				{
					start: time.Unix(50, 0),
					end:   time.Unix(66, 0),
				},
				{
					start: time.Unix(44, 0),
					end:   time.Unix(68, 0),
				},
			},
			input: time.Unix(69, 0),
			expectedWindows: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(65, 0),
				},
				{
					start: time.Unix(50, 0),
					end:   time.Unix(66, 0),
				},
				{
					start: time.Unix(44, 0),
					end:   time.Unix(68, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_DeleteWindow(t *testing.T) {
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
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_InsertFront(t *testing.T) {
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
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_InsertBack(t *testing.T) {
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
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_Insert(t *testing.T) {
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
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
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

func TestSortedWindowListByEndTime_FindWindowForTime(t *testing.T) {
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
			name: "first_present",
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
		{
			name: "multiple_present",
			given: []*TestWindow{
				{
					start: time.Unix(60, 0),
					end:   time.Unix(80, 0),
				},
				{
					start: time.Unix(50, 0),
					end:   time.Unix(90, 0),
				},
				{
					start: time.Unix(40, 0),
					end:   time.Unix(100, 0),
				},
			},
			input: time.Unix(75, 0),
			expectedWindow: &TestWindow{
				start: time.Unix(60, 0),
				end:   time.Unix(80, 0),
			},
			isPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByEndTime()
			setupWindows(windows, tt.given)
			win, isPresent := windows.FindWindowForTime(tt.input)
			assert.Equal(t, tt.isPresent, isPresent)
			if isPresent {
				assert.Equal(t, win, tt.expectedWindow)
			}
		})
	}
}

func TestSortedWindowListByEndTime_WindowToBeMerged(t *testing.T) {
	// 545000-560000-slot-0

	//562000-607000-slot-0
	//581000-608000-slot-0
	//561000-609000-slot-0
	//563000-610000-slot-0
	//557000-611000-slot-0
	//564000-612000-slot-0
	//577000-613000-slot-0
	//560000-614000-slot-0
	//654000-674000-slot-0

	testWin := &TestWindow{
		start: time.Unix(545, 0),
		end:   time.Unix(560, 0),
		slot:  "slot-0",
	}

	win1 := &TestWindow{
		start: time.Unix(562, 0),
		end:   time.Unix(607, 0),
		slot:  "slot-0",
	}

	win2 := &TestWindow{
		start: time.Unix(581, 0),
		end:   time.Unix(608, 0),
		slot:  "slot-0",
	}

	win3 := &TestWindow{
		start: time.Unix(561, 0),
		end:   time.Unix(609, 0),
		slot:  "slot-0",
	}

	win4 := &TestWindow{
		start: time.Unix(563, 0),
		end:   time.Unix(610, 0),
		slot:  "slot-0",
	}

	win5 := &TestWindow{
		start: time.Unix(559, 0),
		end:   time.Unix(611, 0),
		slot:  "slot-0",
	}

	win6 := &TestWindow{
		start: time.Unix(564, 0),
		end:   time.Unix(612, 0),
		slot:  "slot-0",
	}

	win7 := &TestWindow{
		start: time.Unix(577, 0),
		end:   time.Unix(613, 0),
		slot:  "slot-0",
	}

	win8 := &TestWindow{
		start: time.Unix(560, 0),
		end:   time.Unix(614, 0),
		slot:  "slot-0",
	}

	win9 := &TestWindow{
		start: time.Unix(654, 0),
		end:   time.Unix(674, 0),
		slot:  "slot-0",
	}

	windowList := NewSortedWindowListByEndTime()
	windowList.Insert(win1)
	windowList.Insert(win2)
	windowList.Insert(win3)
	windowList.Insert(win4)
	windowList.Insert(win5)
	windowList.Insert(win6)
	windowList.Insert(win7)
	windowList.Insert(win8)
	windowList.Insert(win9)

	outputWin, canBeMerged := windowList.WindowToBeMerged(testWin)
	assert.Equal(t, true, canBeMerged)
	assert.Equal(t, win5.Partition().String(), outputWin.Partition().String())
}

func TestSortedWindowListByEndTime_WindowToBeMerged2(t *testing.T) {
	testWin := &TestWindow{
		start: time.Unix(38, 0),
		end:   time.Unix(48, 0),
		slot:  "slot-0",
	}

	win1 := &TestWindow{
		start: time.Unix(37, 0),
		end:   time.Unix(47, 0),
		slot:  "slot-0",
	}

	windowList := NewSortedWindowListByEndTime()
	windowList.Insert(win1)

	outputWin, canBeMerged := windowList.WindowToBeMerged(testWin)
	assert.Equal(t, true, canBeMerged)
	assert.Equal(t, win1.Partition().String(), outputWin.Partition().String())
}

func setupWindows(windows *SortedWindowListByEndTime, wins []*TestWindow) {
	for _, win := range wins {
		windows.InsertBack(win)
	}
}

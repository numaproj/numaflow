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

type TestWindowEndTime struct {
	start time.Time
	end   time.Time
	slot  string
}

func (t *TestWindowEndTime) StartTime() time.Time {
	return t.start
}

func (t *TestWindowEndTime) EndTime() time.Time {
	return t.end
}

func (t *TestWindowEndTime) Slot() string {
	return t.slot
}

func (t *TestWindowEndTime) Partition() *partition.ID {
	return &partition.ID{
		Start: t.start,
		End:   t.end,
		Slot:  t.slot,
	}
}

func (t *TestWindowEndTime) Merge(tw TimedWindow) {
	//TODO implement me
	panic("implement me")
}

func (t *TestWindowEndTime) Expand(et time.Time) {
	//TODO implement me
	panic("implement me")
}

// TODO add tests to cover different slot values

func TestSortedWindowListByEndTime_InsertIfNotPresent(t *testing.T) {
	tests := []struct {
		name            string
		given           []*TestWindowEndTime
		input           *TestWindowEndTime
		expectedWindows []*TestWindowEndTime
		isPresent       bool
	}{
		{
			name:  "FirstWindow",
			given: []*TestWindowEndTime{},
			input: &TestWindowEndTime{
				start: time.Unix(0, 0),
				end:   time.Unix(60, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(60, 0),
				end:   time.Unix(120, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
					slot:  "slot-0",
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
				slot:  "slot-0",
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
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
			input: &TestWindowEndTime{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			windows := NewSortedWindowListByEndTime[*TestWindowEndTime]()
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
		given           []*TestWindowEndTime
		input           time.Time
		expectedWindows []*TestWindowEndTime
	}{
		{
			name: "remove_windows",
			given: []*TestWindowEndTime{
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
			expectedWindows: []*TestWindowEndTime{
				{
					start: time.Unix(120, 0),
					end:   time.Unix(180, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByEndTime[*TestWindowEndTime]()
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
		given           []*TestWindowEndTime
		input           *TestWindowEndTime
		expectedWindows []*TestWindowEndTime
	}{
		{
			name: "delete_middle",
			given: []*TestWindowEndTime{
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
			input: &TestWindowEndTime{
				start: time.Unix(180, 0),
				end:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
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
			input: &TestWindowEndTime{
				start: time.Unix(120, 0),
				end:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
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
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			windows := NewSortedWindowListByEndTime[*TestWindowEndTime]()
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
		given           []*TestWindowEndTime
		input           *TestWindowEndTime
		expectedWindows []*TestWindowEndTime
	}{
		{
			name:  "insert_empty",
			given: []*TestWindowEndTime{},
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(300, 0),
				end:   time.Unix(360, 0),
			},
			expectedWindows: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByEndTime[*TestWindowEndTime]()
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
		given           []*TestWindowEndTime
		input           *TestWindowEndTime
		expectedWindows []*TestWindowEndTime
	}{
		{
			name:  "insert_empty",
			given: []*TestWindowEndTime{},
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(240, 0),
				end:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindowEndTime{
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
			given: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
			input: &TestWindowEndTime{
				start: time.Unix(120, 0),
				end:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindowEndTime{
				{
					start: time.Unix(240, 0),
					end:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowListByEndTime[*TestWindowEndTime]()
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

func setupWindows(windows *SortedWindowListByEndTime[*TestWindowEndTime], wins []*TestWindowEndTime) {
	for _, win := range wins {
		windows.InsertBack(win)
	}
}

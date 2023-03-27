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
)

type TestWindow struct {
	Start time.Time
	End   time.Time
}

func (t *TestWindow) StartTime() time.Time {
	return t.Start
}

func (t *TestWindow) EndTime() time.Time {
	return t.End
}

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
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(0, 0),
					End:   time.Unix(60, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "late_window",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(60, 0),
				End:   time.Unix(120, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(60, 0),
					End:   time.Unix(120, 0),
				},
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "early_window",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "insert_middle",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			isPresent: false,
		},
		{
			name: "already_present",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			isPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowList[*TestWindow]()
			setup(windows, tt.given)
			ret, isPresent := windows.InsertIfNotPresent(tt.input)
			assert.Equal(t, tt.isPresent, isPresent)
			assert.Equal(t, tt.input.Start, ret.StartTime())
			assert.Equal(t, tt.input.End, ret.EndTime())
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
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: time.Unix(180, 0),
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowList[*TestWindow]()
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
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "delete_first",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(120, 0),
				End:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "delete_last",
			given: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
				{
					Start: time.Unix(180, 0),
					End:   time.Unix(240, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowList[*TestWindow]()
			setup(windows, tt.given)
			windows.DeleteWindow(tt.input)
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
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_greater",
			given: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(300, 0),
				End:   time.Unix(360, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowList[*TestWindow]()
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
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_equal",
			given: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
		{
			name: "insert_smaller",
			given: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
			input: &TestWindow{
				Start: time.Unix(120, 0),
				End:   time.Unix(180, 0),
			},
			expectedWindows: []*TestWindow{
				{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := NewSortedWindowList[*TestWindow]()
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

func setup(windows *SortedWindowList[*TestWindow], wins []*TestWindow) {
	for _, win := range wins {
		windows.InsertBack(win)
	}
}

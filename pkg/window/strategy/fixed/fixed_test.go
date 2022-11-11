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
	"container/list"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

func TestFixed_AssignWindow(t *testing.T) {

	loc, _ := time.LoadLocation("UTC")
	baseTime := time.Unix(1651129201, 0).In(loc)

	tests := []struct {
		name      string
		length    time.Duration
		eventTime time.Time
		want      []*window.IntervalWindow
	}{
		{
			name:      "minute",
			length:    time.Minute,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129260, 0).In(loc),
				},
			},
		},
		{
			name:      "hour",
			length:    time.Hour,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+3600, 0).In(loc),
				},
			},
		},
		{
			name:      "5_minute",
			length:    time.Minute * 5,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+300, 0).In(loc),
				},
			},
		},
		{
			name:      "30_second",
			length:    time.Second * 30,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129230, 0).In(loc),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFixed(tt.length)
			if got := f.AssignWindow(tt.eventTime); !(got[0].Start.Equal(tt.want[0].Start) && got[0].End.Equal(tt.want[0].End)) {
				t.Errorf("AssignWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAligned_CreateWindow tests the insertion of a new keyed window for a given interval
// It tests early, late and existing window scenarios.
func TestAligned_CreateWindow(t *testing.T) {
	windows := NewFixed(60 * time.Second)
	tests := []struct {
		name            string
		given           []*keyed.KeyedWindow
		input           *window.IntervalWindow
		expectedWindows []*keyed.KeyedWindow
	}{
		{
			name:  "FirstWindow",
			given: []*keyed.KeyedWindow{},
			input: &window.IntervalWindow{
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(0, 0),
						End:   time.Unix(60, 0),
					},
				},
			},
		},
		{
			name: "late_window",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(60, 0),
				End:   time.Unix(120, 0),
			},
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(60, 0),
						End:   time.Unix(120, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
		},
		{
			name: "early_window",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
		},
		{
			name: "insert_middle",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(180, 0),
						End:   time.Unix(240, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows, tt.given)
			ret := windows.CreateWindow(tt.input)
			assert.Equal(t, tt.input.Start, ret.StartTime())
			assert.Equal(t, tt.input.End, ret.EndTime())
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.Start, node.Value.(*keyed.KeyedWindow).Start)
				assert.Equal(t, kw.End, node.Value.(*keyed.KeyedWindow).End)
				node = node.Next()
			}
		})
	}
}

func TestAligned_GetWindow(t *testing.T) {
	windows := NewFixed(60 * time.Second)
	tests := []struct {
		name           string
		given          []*keyed.KeyedWindow
		input          *window.IntervalWindow
		expectedWindow *keyed.KeyedWindow
	}{
		{
			name:  "non_existing",
			given: []*keyed.KeyedWindow{},
			input: &window.IntervalWindow{
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindow: nil,
		},
		{
			name: "non_existing_before_earlier",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(60, 0),
						End:   time.Unix(120, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindow: nil,
		},
		{
			name: "non_existing_after_recent",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(60, 0),
						End:   time.Unix(120, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindow: nil,
		},
		{
			name: "non_existing_middle",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(60, 0),
						End:   time.Unix(120, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(180, 0),
						End:   time.Unix(240, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(120, 0),
				End:   time.Unix(180, 0),
			},
			expectedWindow: nil,
		},
		{
			name: "existing",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(60, 0),
						End:   time.Unix(120, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(60, 0),
				End:   time.Unix(120, 0),
			},
			expectedWindow: &keyed.KeyedWindow{
				IntervalWindow: &window.IntervalWindow{
					Start: time.Unix(60, 0),
					End:   time.Unix(120, 0),
				},
			},
		},
		{
			name: "first_window",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(120, 0),
				End:   time.Unix(180, 0),
			},
			expectedWindow: &keyed.KeyedWindow{
				IntervalWindow: &window.IntervalWindow{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
		},
		{
			name: "last_window",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindow: &keyed.KeyedWindow{
				IntervalWindow: &window.IntervalWindow{
					Start: time.Unix(240, 0),
					End:   time.Unix(300, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows, tt.given)
			ret := windows.GetWindow(tt.input)
			if tt.expectedWindow == nil {
				assert.Nil(t, ret)
			} else {
				assert.Equal(t, tt.input.StartTime(), ret.StartTime())
				assert.Equal(t, tt.input.EndTime(), ret.EndTime())
				assert.Equal(t, tt.expectedWindow.StartTime(), ret.StartTime())
				assert.Equal(t, tt.expectedWindow.EndTime(), ret.EndTime())
			}
		})
	}
}

func TestAligned_RemoveWindow(t *testing.T) {
	windows := NewFixed(60 * time.Second)
	tests := []struct {
		name            string
		given           []*keyed.KeyedWindow
		input           time.Time
		expectedWindows []*keyed.KeyedWindow
	}{
		{
			name:            "empty_windows",
			given:           []*keyed.KeyedWindow{},
			input:           time.Unix(60, 0),
			expectedWindows: []*keyed.KeyedWindow{},
		},
		{
			name: "wm_on_edge",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input:           time.Unix(180, 0),
			expectedWindows: []*keyed.KeyedWindow{},
		},
		{
			name: "single_window",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: time.Unix(181, 0),
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
		},
		{
			name: "single_when_multiple",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(180, 0),
						End:   time.Unix(240, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
			input: time.Unix(181, 0),
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
		},
		{
			name: "multiple_removals",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(180, 0),
						End:   time.Unix(240, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(240, 0),
						End:   time.Unix(300, 0),
					},
				},
			},
			input: time.Unix(245, 0),
			expectedWindows: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(180, 0),
						End:   time.Unix(240, 0),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows, tt.given)
			ret := windows.RemoveWindows(tt.input)
			assert.Equal(t, len(tt.expectedWindows), len(ret))
			for idx, kw := range tt.expectedWindows {
				assert.Equal(t, kw.Start, ret[idx].StartTime())
				assert.Equal(t, kw.End, ret[idx].EndTime())
			}
		})
	}
}

func setup(windows *Fixed, wins []*keyed.KeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

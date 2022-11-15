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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

func TestFixed_AssignWindow(t *testing.T) {

	loc, _ := time.LoadLocation("UTC")
	baseTime := time.Unix(1651129201, 0).In(loc)

	tests := []struct {
		name      string
		length    time.Duration
		eventTime time.Time
		want      []window.AlignedKeyedWindower
	}{
		{
			name:      "minute",
			length:    time.Minute,
			eventTime: baseTime,
			want: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(1651129200, 0).In(loc), time.Unix(1651129260, 0).In(loc)),
			},
		},
		{
			name:      "hour",
			length:    time.Hour,
			eventTime: baseTime,
			want: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(1651129200, 0).In(loc), time.Unix(1651129200+3600, 0).In(loc)),
			},
		},
		{
			name:      "5_minute",
			length:    time.Minute * 5,
			eventTime: baseTime,
			want: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(1651129200, 0).In(loc), time.Unix(1651129200+300, 0).In(loc)),
			},
		},
		{
			name:      "30_second",
			length:    time.Second * 30,
			eventTime: baseTime,
			want: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(1651129200, 0).In(loc), time.Unix(1651129230, 0).In(loc)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFixed(tt.length)
			if got := f.AssignWindow(tt.eventTime); !(got[0].StartTime().Equal(tt.want[0].StartTime()) && got[0].EndTime().Equal(tt.want[0].EndTime())) {
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
		given           []*keyed.AlignedKeyedWindow
		input           *keyed.AlignedKeyedWindow
		expectedWindows []window.AlignedKeyedWindower
	}{
		{
			name:  "FirstWindow",
			given: []*keyed.AlignedKeyedWindow{},
			input: keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
			},
		},
		{
			name: "late_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
		},
		{
			name: "early_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input: &keyed.AlignedKeyedWindow{
				Start: time.Unix(240, 0),
				End:   time.Unix(300, 0),
			},
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
		},
		{
			name: "insert_middle",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			input: &keyed.AlignedKeyedWindow{
				Start: time.Unix(180, 0),
				End:   time.Unix(240, 0),
			},
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := windows.(*Fixed)
			setup(windows, tt.given)
			ret := windows.CreateWindow(tt.input)
			assert.Equal(t, tt.input.Start, ret.StartTime())
			assert.Equal(t, tt.input.End, ret.EndTime())
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), node.Value.(*keyed.AlignedKeyedWindow).StartTime())
				assert.Equal(t, kw.EndTime(), node.Value.(*keyed.AlignedKeyedWindow).EndTime())
				node = node.Next()
			}
		})
	}
}

func TestAligned_GetWindow(t *testing.T) {
	windows := NewFixed(60 * time.Second)
	tests := []struct {
		name           string
		given          []*keyed.AlignedKeyedWindow
		input          *keyed.AlignedKeyedWindow
		expectedWindow *keyed.AlignedKeyedWindow
	}{
		{
			name:  "non_existing",
			given: []*keyed.AlignedKeyedWindow{},
			input: keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),

			expectedWindow: nil,
		},
		{
			name: "non_existing_before_earlier",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
			expectedWindow: nil,
		},
		{
			name: "non_existing_after_recent",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
			expectedWindow: nil,
		},
		{
			name: "non_existing_middle",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
				keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			expectedWindow: nil,
		},
		{
			name: "existing",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
			expectedWindow: keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
		},
		{
			name: "first_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			expectedWindow: keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
		},
		{
			name: "last_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			input:          keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			expectedWindow: keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows.(*Fixed), tt.given)
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
		given           []*keyed.AlignedKeyedWindow
		input           time.Time
		expectedWindows []window.AlignedKeyedWindower
	}{
		{
			name:            "empty_windows",
			given:           []*keyed.AlignedKeyedWindow{},
			input:           time.Unix(60, 0),
			expectedWindows: []window.AlignedKeyedWindower{},
		},
		{
			name: "wm_on_edge",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input:           time.Unix(180, 0),
			expectedWindows: []window.AlignedKeyedWindower{},
		},
		{
			name: "single_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input: time.Unix(181, 0),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
		},
		{
			name: "single_when_multiple",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			input: time.Unix(181, 0),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
		},
		{
			name: "multiple_removals",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			input: time.Unix(245, 0),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows.(*Fixed), tt.given)
			ret := windows.RemoveWindows(tt.input)
			assert.Equal(t, len(tt.expectedWindows), len(ret))
			for idx, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), ret[idx].StartTime())
				assert.Equal(t, kw.EndTime(), ret[idx].EndTime())
			}
		})
	}
}

func setup(windows *Fixed, wins []*keyed.AlignedKeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

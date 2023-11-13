// /*
// Copyright 2022 The Numaproj Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */
package sliding

//
//import (
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//
//	"github.com/numaproj/numaflow/pkg/window"
//	"github.com/numaproj/numaflow/pkg/window/keyed"
//)
//
//// TestSliding_AssignWindow tests the assignment of element to a set of windows
//func TestSliding_AssignWindow(t *testing.T) {
//	baseTime := time.Unix(600, 0)
//
//	tests := []struct {
//		name      string
//		length    time.Duration
//		slide     time.Duration
//		eventTime time.Time
//		expected  []window.AlignedKeyedWindower
//	}{
//		{
//			name:      "length divisible by slide",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(10 * time.Second),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
//			},
//		},
//		{
//			name:      "length not divisible by slide",
//			length:    time.Minute,
//			slide:     40 * time.Second,
//			eventTime: baseTime.Add(10 * time.Second),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
//			},
//		},
//		{
//			name:      "prime slide",
//			length:    time.Minute,
//			slide:     41 * time.Second,
//			eventTime: baseTime.Add(10 * time.Second),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(574, 0), time.Unix(634, 0)),
//			},
//		},
//		{
//			name:      "element eq start time",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime,
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
//			},
//		},
//		{
//			name:      "element eq end time",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(time.Minute),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(660, 0), time.Unix(720, 0)),
//				keyed.NewKeyedWindow(time.Unix(640, 0), time.Unix(700, 0)),
//				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
//			},
//		},
//		{
//			name:      "element on right",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(time.Nanosecond),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
//			},
//		},
//		{
//			name:      "element on left",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(-time.Nanosecond),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
//				keyed.NewKeyedWindow(time.Unix(540, 0), time.Unix(600, 0)),
//			},
//		},
//		{
//			name:      "element on a window boundary",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(20 * time.Second),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//			},
//		},
//		{
//			name:      "element on a window boundary plus one",
//			length:    time.Minute,
//			slide:     20 * time.Second,
//			eventTime: baseTime.Add(21 * time.Second),
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
//				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
//				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
//			},
//		},
//		{
//			name:      "length not divisible by slide test 1",
//			length:    time.Second * 600,
//			slide:     70 * time.Second,
//			eventTime: baseTime.Add(210 * time.Second), // 810
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(770, 0), time.Unix(1370, 0)),
//				keyed.NewKeyedWindow(time.Unix(700, 0), time.Unix(1300, 0)),
//				keyed.NewKeyedWindow(time.Unix(630, 0), time.Unix(1230, 0)),
//				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(1160, 0)),
//				keyed.NewKeyedWindow(time.Unix(490, 0), time.Unix(1090, 0)),
//				keyed.NewKeyedWindow(time.Unix(420, 0), time.Unix(1020, 0)),
//				keyed.NewKeyedWindow(time.Unix(350, 0), time.Unix(950, 0)),
//				keyed.NewKeyedWindow(time.Unix(280, 0), time.Unix(880, 0)),
//			},
//		},
//		{
//			name:      "length not divisible by slide test 2",
//			length:    time.Second * 600,
//			slide:     70 * time.Second,
//			eventTime: baseTime.Add(610 * time.Second), // 1210
//			expected: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(1190, 0), time.Unix(1790, 0)),
//				keyed.NewKeyedWindow(time.Unix(1120, 0), time.Unix(1720, 0)),
//				keyed.NewKeyedWindow(time.Unix(1050, 0), time.Unix(1650, 0)),
//				keyed.NewKeyedWindow(time.Unix(980, 0), time.Unix(1580, 0)),
//				keyed.NewKeyedWindow(time.Unix(910, 0), time.Unix(1510, 0)),
//				keyed.NewKeyedWindow(time.Unix(840, 0), time.Unix(1440, 0)),
//				keyed.NewKeyedWindow(time.Unix(770, 0), time.Unix(1370, 0)),
//				keyed.NewKeyedWindow(time.Unix(700, 0), time.Unix(1300, 0)),
//				keyed.NewKeyedWindow(time.Unix(630, 0), time.Unix(1230, 0)),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			s := NewSliding(tt.length, tt.slide)
//			got := s.AssignWindow(tt.eventTime)
//			assert.Len(t, got, len(tt.expected))
//			assert.Equal(t, tt.expected, got)
//		})
//	}
//}
//
//func TestAligned_CreateWindow(t *testing.T) {
//	windows := NewSliding(60*time.Second, 20*time.Second)
//	tests := []struct {
//		name            string
//		given           []*keyed.AlignedKeyedWindow
//		input           *keyed.AlignedKeyedWindow
//		expectedWindows []window.AlignedKeyedWindower
//		isPresent       bool
//	}{
//		{
//			name:  "FirstWindow",
//			given: []*keyed.AlignedKeyedWindow{},
//			input: keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
//			},
//			isPresent: false,
//		},
//		{
//			name: "overlapping_windows",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//			},
//			isPresent: false,
//		},
//		{
//			name: "early_window",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
//			},
//			isPresent: false,
//		},
//		{
//			name: "insert_middle",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			isPresent: false,
//		},
//		{
//			name: "existing_windows",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			isPresent: true,
//		},
//		{
//			name: "existing_early_window",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			isPresent: true,
//		},
//		{
//			name: "existing_latest_window",
//			given: []*keyed.AlignedKeyedWindow{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			input: keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			expectedWindows: []window.AlignedKeyedWindower{
//				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
//				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
//			},
//			isPresent: true,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			setup(windows, tt.given)
//			ret, isNew := windows.InsertIfNotPresent(tt.input)
//			assert.Equal(t, isNew, tt.isPresent)
//			assert.Equal(t, tt.input.StartTime(), ret.StartTime())
//			assert.Equal(t, tt.input.EndTime(), ret.EndTime())
//			assert.Equal(t, len(tt.expectedWindows), windows.activeWindows.Len())
//			nodes := windows.activeWindows.Items()
//			i := 0
//			for _, kw := range tt.expectedWindows {
//				assert.Equal(t, kw.StartTime(), nodes[i].StartTime())
//				assert.Equal(t, kw.EndTime(), nodes[i].EndTime())
//				i += 1
//			}
//		})
//	}
//}
//
//func TestSliding_RemoveWindows(t *testing.T) {
//	var (
//		length          = time.Second * 60
//		slide           = time.Second * 10
//		slidWin         = NewSliding(length, slide)
//		eventTime       = time.Unix(60, 0)
//		expectedWindows = []window.AlignedKeyedWindower{
//			keyed.NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0)),
//			keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
//			keyed.NewKeyedWindow(time.Unix(180, 0), time.Unix(240, 0)),
//			keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
//		}
//	)
//	for i := 0; i < 10000; i++ {
//		win := keyed.NewKeyedWindow(eventTime, eventTime.Add(length))
//		slidWin.InsertIfNotPresent(win)
//		eventTime = eventTime.Add(length)
//	}
//	closeWin := slidWin.RemoveWindows(time.Unix(300, 0))
//	assert.Equal(t, closeWin, expectedWindows)
//}
//
//func setup(windows *Sliding, wins []*keyed.AlignedKeyedWindow) {
//	windows.activeWindows = window.NewSortedWindowList[window.AlignedKeyedWindower]()
//	for _, win := range wins {
//		windows.activeWindows.InsertBack(win)
//	}
//}

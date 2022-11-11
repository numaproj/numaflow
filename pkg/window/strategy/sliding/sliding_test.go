package sliding

import (
	"container/list"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestSliding_AssignWindow tests the assignment of element to a set of windows
func TestSliding_AssignWindow(t *testing.T) {
	baseTime := time.Unix(600, 0)

	tests := []struct {
		name      string
		length    time.Duration
		slide     time.Duration
		eventTime time.Time
		expected  []*window.IntervalWindow
	}{
		{
			name:      "length divisible by slide",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(560, 0),
					End:   time.Unix(620, 0),
				},
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
			},
		},
		{
			name:      "length not divisible by slide",
			length:    time.Minute,
			slide:     40 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(560, 0),
					End:   time.Unix(620, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
			},
		},
		{
			name:      "prime slide",
			length:    time.Minute,
			slide:     41 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(559, 0),
					End:   time.Unix(619, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
			},
		},
		{
			name:      "element eq start time",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime,
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(560, 0),
					End:   time.Unix(620, 0),
				},
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
			},
		},
		{
			name:      "element eq end time",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(time.Minute),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(620, 0),
					End:   time.Unix(680, 0),
				},
				{
					Start: time.Unix(640, 0),
					End:   time.Unix(700, 0),
				},
				{
					Start: time.Unix(660, 0),
					End:   time.Unix(720, 0),
				},
			},
		},
		{
			name:      "element on right",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(time.Nanosecond),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(560, 0),
					End:   time.Unix(620, 0),
				},
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
			},
		},
		{
			name:      "element on left",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(-time.Nanosecond),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(540, 0),
					End:   time.Unix(600, 0),
				},
				{
					Start: time.Unix(560, 0),
					End:   time.Unix(620, 0),
				},
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
			},
		},
		{
			name:      "element on a window boundary",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(20 * time.Second),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
				{
					Start: time.Unix(620, 0),
					End:   time.Unix(680, 0),
				},
			},
		},
		{
			name:      "element on a window boundary plus one",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(21 * time.Second),
			expected: []*window.IntervalWindow{
				{
					Start: time.Unix(580, 0),
					End:   time.Unix(640, 0),
				},
				{
					Start: time.Unix(600, 0),
					End:   time.Unix(660, 0),
				},
				{
					Start: time.Unix(620, 0),
					End:   time.Unix(680, 0),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSliding(tt.length, tt.slide)
			got := s.AssignWindow(tt.eventTime)
			assert.Len(t, got, len(tt.expected))
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestAligned_CreateWindow(t *testing.T) {
	windows := NewSliding(60*time.Second, 20*time.Second)
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
			name: "overlapping_windows",
			given: []*keyed.KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(140, 0),
				End:   time.Unix(200, 0),
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
						Start: time.Unix(140, 0),
						End:   time.Unix(200, 0),
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
						Start: time.Unix(160, 0),
						End:   time.Unix(220, 0),
					},
				},
			},
			input: &window.IntervalWindow{
				Start: time.Unix(140, 0),
				End:   time.Unix(200, 0),
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
						Start: time.Unix(140, 0),
						End:   time.Unix(200, 0),
					},
				},
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(160, 0),
						End:   time.Unix(220, 0),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows, tt.given)
			t.Log("setup done")
			ret := windows.CreateWindow(tt.input)
			t.Log("created window")
			assert.Equal(t, tt.input.StartTime(), ret.StartTime())
			assert.Equal(t, tt.input.EndTime(), ret.EndTime())
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), node.Value.(*keyed.KeyedWindow).Start)
				assert.Equal(t, kw.EndTime(), node.Value.(*keyed.KeyedWindow).End)
				node = node.Next()
			}
		})
	}
}

func setup(windows *Sliding, wins []*keyed.KeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

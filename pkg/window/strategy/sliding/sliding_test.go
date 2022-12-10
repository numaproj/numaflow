package sliding

import (
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/keyed"
)

// TestSliding_AssignWindow tests the assignment of element to a set of windows
func TestSliding_AssignWindow(t *testing.T) {
	baseTime := time.Unix(600, 0)

	tests := []struct {
		name      string
		length    time.Duration
		slide     time.Duration
		eventTime time.Time
		expected  []window.AlignedKeyedWindower
	}{
		{
			name:      "length divisible by slide",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
			},
		},
		{
			name:      "length not divisible by slide",
			length:    time.Minute,
			slide:     40 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
			},
		},
		{
			name:      "prime slide",
			length:    time.Minute,
			slide:     41 * time.Second,
			eventTime: baseTime.Add(10 * time.Second),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(559, 0), time.Unix(619, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
			},
		},
		{
			name:      "element eq start time",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime,
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
			},
		},
		{
			name:      "element eq end time",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(time.Minute),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
				keyed.NewKeyedWindow(time.Unix(640, 0), time.Unix(700, 0)),
				keyed.NewKeyedWindow(time.Unix(660, 0), time.Unix(720, 0)),
			},
		},
		{
			name:      "element on right",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(time.Nanosecond),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
			},
		},
		{
			name:      "element on left",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(-time.Nanosecond),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(540, 0), time.Unix(600, 0)),
				keyed.NewKeyedWindow(time.Unix(560, 0), time.Unix(620, 0)),
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
			},
		},
		{
			name:      "element on a window boundary",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(20 * time.Second),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
			},
		},
		{
			name:      "element on a window boundary plus one",
			length:    time.Minute,
			slide:     20 * time.Second,
			eventTime: baseTime.Add(21 * time.Second),
			expected: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(580, 0), time.Unix(640, 0)),
				keyed.NewKeyedWindow(time.Unix(600, 0), time.Unix(660, 0)),
				keyed.NewKeyedWindow(time.Unix(620, 0), time.Unix(680, 0)),
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
		given           []*keyed.AlignedKeyedWindow
		input           *keyed.AlignedKeyedWindow
		expectedWindows []window.AlignedKeyedWindower
		isPresent       bool
	}{
		{
			name:  "FirstWindow",
			given: []*keyed.AlignedKeyedWindow{},
			input: keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(0, 0), time.Unix(60, 0)),
			},
			isPresent: false,
		},
		{
			name: "overlapping_windows",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
			},
			isPresent: false,
		},
		{
			name: "early_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(240, 0), time.Unix(300, 0)),
			},
			isPresent: false,
		},
		{
			name: "insert_middle",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			isPresent: false,
		},
		{
			name: "existing_windows",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			isPresent: true,
		},
		{
			name: "existing_early_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			isPresent: true,
		},
		{
			name: "existing_latest_window",
			given: []*keyed.AlignedKeyedWindow{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			input: keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			expectedWindows: []window.AlignedKeyedWindower{
				keyed.NewKeyedWindow(time.Unix(120, 0), time.Unix(180, 0)),
				keyed.NewKeyedWindow(time.Unix(140, 0), time.Unix(200, 0)),
				keyed.NewKeyedWindow(time.Unix(160, 0), time.Unix(220, 0)),
			},
			isPresent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(windows, tt.given)
			ret, isNew := windows.InsertIfNotPresent(tt.input)
			assert.Equal(t, isNew, tt.isPresent)
			assert.Equal(t, tt.input.StartTime(), ret.StartTime())
			assert.Equal(t, tt.input.EndTime(), ret.EndTime())
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.StartTime(), node.Value.(*keyed.AlignedKeyedWindow).Start)
				assert.Equal(t, kw.EndTime(), node.Value.(*keyed.AlignedKeyedWindow).End)
				node = node.Next()
			}
		})
	}
}

func setup(windows *Sliding, wins []*keyed.AlignedKeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

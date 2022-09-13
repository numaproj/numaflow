package aligned

import (
	"container/list"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestAligned_CreateWindow tests the insertion of a new keyed window for a given interval
// It tests early, late and existing window scenarios.
func TestAligned_CreateWindow(t *testing.T) {
	windows := NewWindows()
	tests := []struct {
		name            string
		given           []*KeyedWindow
		input           *window.IntervalWindow
		expectedWindows []*KeyedWindow
	}{
		{
			name:  "FirstWindow",
			given: []*KeyedWindow{},
			input: &window.IntervalWindow{
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindows: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindows: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindows: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindows: []*KeyedWindow{
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
			ret := windows.CreateKeyedWindow(tt.input)
			assert.Equal(t, tt.input.Start, ret.Start)
			assert.Equal(t, tt.input.End, ret.End)
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				assert.Equal(t, kw.Start, node.Value.(*KeyedWindow).Start)
				assert.Equal(t, kw.End, node.Value.(*KeyedWindow).End)
				node = node.Next()
			}
		})
	}
}

func TestAligned_GetWindow(t *testing.T) {
	windows := NewWindows()
	tests := []struct {
		name           string
		given          []*KeyedWindow
		input          *window.IntervalWindow
		expectedWindow *KeyedWindow
	}{
		{
			name:  "non_existing",
			given: []*KeyedWindow{},
			input: &window.IntervalWindow{
				Start: time.Unix(0, 0),
				End:   time.Unix(60, 0),
			},
			expectedWindow: nil,
		},
		{
			name: "non_existing_before_earlier",
			given: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindow: &KeyedWindow{
				IntervalWindow: &window.IntervalWindow{
					Start: time.Unix(60, 0),
					End:   time.Unix(120, 0),
				},
			},
		},
		{
			name: "first_window",
			given: []*KeyedWindow{
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
			expectedWindow: &KeyedWindow{
				IntervalWindow: &window.IntervalWindow{
					Start: time.Unix(120, 0),
					End:   time.Unix(180, 0),
				},
			},
		},
		{
			name: "last_window",
			given: []*KeyedWindow{
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
			expectedWindow: &KeyedWindow{
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
			ret := windows.GetKeyedWindow(tt.input)
			if tt.expectedWindow == nil {
				assert.Nil(t, ret)
			} else {
				assert.Equal(t, tt.input.Start, ret.Start)
				assert.Equal(t, tt.input.End, ret.End)
				assert.Equal(t, tt.expectedWindow.Start, ret.Start)
				assert.Equal(t, tt.expectedWindow.End, ret.End)
			}
		})
	}
}

func TestAligned_RemoveWindow(t *testing.T) {
	windows := NewWindows()
	tests := []struct {
		name            string
		given           []*KeyedWindow
		input           time.Time
		expectedWindows []*KeyedWindow
	}{
		{
			name:            "empty_windows",
			given:           []*KeyedWindow{},
			input:           time.Unix(60, 0),
			expectedWindows: []*KeyedWindow{},
		},
		{
			name: "wm_on_edge",
			given: []*KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input:           time.Unix(180, 0),
			expectedWindows: []*KeyedWindow{},
		},
		{
			name: "single_window",
			given: []*KeyedWindow{
				{
					IntervalWindow: &window.IntervalWindow{
						Start: time.Unix(120, 0),
						End:   time.Unix(180, 0),
					},
				},
			},
			input: time.Unix(181, 0),
			expectedWindows: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindows: []*KeyedWindow{
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
			given: []*KeyedWindow{
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
			expectedWindows: []*KeyedWindow{
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
			ret := windows.RemoveWindow(tt.input)
			assert.Equal(t, len(tt.expectedWindows), len(ret))
			for idx, kw := range tt.expectedWindows {
				assert.Equal(t, kw.Start, ret[idx].Start)
				assert.Equal(t, kw.End, ret[idx].End)
			}
		})
	}
}

func setup(windows *ActiveWindows, wins []*KeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

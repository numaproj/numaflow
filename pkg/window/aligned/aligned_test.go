package aligned

import (
	"container/list"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAligned_WindowAdd(t *testing.T) {
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
			ret := windows.CreateOrGetAlignedWindow(tt.input)
			assert.Equal(t, tt.input.Start, ret.Start)
			assert.Equal(t, tt.input.End, ret.End)
			assert.Equal(t, len(tt.expectedWindows), windows.entries.Len())
			node := windows.entries.Front()
			for _, kw := range tt.expectedWindows {
				t.Log(node.Value.(*KeyedWindow).Start, node.Value.(*KeyedWindow).End)
				t.Log(kw.Start, kw.End)
				assert.True(t, isEqual(kw, node.Value.(*KeyedWindow)))
				node = node.Next()
			}
		})
	}
}

func isEqual(kw1 *KeyedWindow, kw2 *KeyedWindow) bool {
	equalStart := kw1.Start == kw2.Start
	equalEnd := kw2.End == kw2.End

	if !(equalEnd && equalStart) {
		return false
	}

	if len(kw1.Keys) != len(kw2.Keys) {
		return false
	}

	for idx, _ := range kw1.Keys {
		if kw1.Keys[idx] != kw2.Keys[idx] {
			return false
		}
	}

	return true
}

func setup(windows *Windows, wins []*KeyedWindow) {
	windows.entries = list.New()
	for _, win := range wins {
		windows.entries.PushBack(win)
	}
}

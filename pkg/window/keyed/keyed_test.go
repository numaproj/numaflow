package keyed

import (
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/stretchr/testify/assert"
)

func TestKeyedWindow_AddKey(t *testing.T) {
	iw := &window.IntervalWindow{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
	}
	kw := NewKeyedWindow(iw)
	tests := []struct {
		name         string
		given        *KeyedWindow
		input        string
		expectedKeys []string
	}{
		{
			name:         "no_keys",
			given:        &KeyedWindow{},
			input:        "key1",
			expectedKeys: []string{"key1"},
		},
		{
			name: "with_some_existing_keys",
			given: &KeyedWindow{
				Keys: []string{"key2", "key3"},
			},
			input:        "key4",
			expectedKeys: []string{"key2", "key3", "key4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw = NewKeyedWindow(iw)
			kw.Keys = append(kw.Keys, tt.given.Keys...)
			kw.AddKey(tt.input)
			assert.ElementsMatch(t, kw.Keys, tt.expectedKeys)
		})
	}
}

func TestKeyedWindow_Partitions(t *testing.T) {
	iw := &window.IntervalWindow{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
	}
	kw := NewKeyedWindow(iw)
	tests := []struct {
		name     string
		given    *KeyedWindow
		input    string
		expected []string
	}{
		{
			name:     "no_keys",
			given:    &KeyedWindow{},
			expected: []string{},
		},
		{
			name: "with_some_existing_keys",
			given: &KeyedWindow{
				Keys: []string{"key2", "key3"},
			},
			expected: []string{
				"key2-60-120",
				"key3-60-120",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw.Keys = tt.given.Keys
			ret := kw.Partitions()
			for idx, s := range tt.expected {
				assert.EqualValues(t, ret[idx], s)
			}
		})
	}
}

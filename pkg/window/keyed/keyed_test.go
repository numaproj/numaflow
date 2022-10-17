package keyed

import (
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/pbq/partition"

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
		expectedKeys map[string]string
	}{
		{
			name:         "no_keys",
			given:        &KeyedWindow{},
			input:        "key1",
			expectedKeys: map[string]string{"key1": "key1"},
		},
		{
			name: "with_some_existing_keys",
			given: &KeyedWindow{
				Keys: map[string]string{"key2": "key2", "key3": "key3"},
			},
			input:        "key4",
			expectedKeys: map[string]string{"key2": "key2", "key3": "key3", "key4": "key4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw = NewKeyedWindow(iw)
			for k := range tt.given.Keys {
				kw.AddKey(k)
			}
			kw.AddKey(tt.input)
			for k := range tt.expectedKeys {
				_, ok := kw.Keys[k]
				assert.True(t, ok)
			}
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
		expected []partition.ID
	}{
		{
			name:     "no_keys",
			given:    &KeyedWindow{},
			expected: []partition.ID{},
		},
		{
			name: "with_some_existing_keys",
			given: &KeyedWindow{
				Keys: map[string]string{"key2": "key2", "key3": "key3"},
			},
			expected: []partition.ID{
				{
					Key:   "key2",
					Start: time.Unix(60, 0),
					End:   time.Unix(120, 0),
				},
				{
					Key:   "key3",
					Start: time.Unix(60, 0),
					End:   time.Unix(120, 0),
				},
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

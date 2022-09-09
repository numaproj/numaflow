package aligned

import (
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
			for _, key := range tt.given.Keys {
				kw.Keys = append(kw.Keys, key)
			}
			kw.AddKey(tt.input)
			assert.True(t, compareStringArr(kw.Keys, tt.expectedKeys))
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
				assert.True(t, stringerAndEqCheck(ret[idx], s))
			}
		})
	}
}

func stringerAndEqCheck(par PartitionId, s string) bool {
	p := string(par)
	return p == s
}

func compareStringArr(o []string, t []string) bool {
	for idx, s := range o {
		if t[idx] != s {
			return false
		}
	}

	return true
}

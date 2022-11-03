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

package keyed

import (
	"sort"
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
			assert.Equal(t, len(tt.expectedKeys), len(kw.Keys))
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
				Keys: map[string]string{"key2": "key2", "key3": "key3", "key4": "key4"},
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
				{
					Key:   "key4",
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
			// the kw.Keys is a map so the order of the output is random
			// use sort to sort the ret array by key
			sort.Slice(ret, func(i int, j int) bool {
				return ret[i].Key < ret[j].Key
			})
			for idx, s := range tt.expected {
				assert.EqualValues(t, ret[idx], s)
			}
		})
	}
}

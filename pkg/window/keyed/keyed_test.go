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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKeyedWindow_AddKey(t *testing.T) {
	kw := NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0))
	tests := []struct {
		name         string
		given        *AlignedKeyedWindow
		input        string
		expectedKeys map[string]struct{}
	}{
		{
			name:         "no_keys",
			given:        &AlignedKeyedWindow{},
			input:        "key1",
			expectedKeys: map[string]struct{}{"key1": {}},
		},
		{
			name: "with_some_existing_keys",
			given: &AlignedKeyedWindow{
				slots: map[string]struct{}{"key2": {}, "key3": {}},
			},
			input:        "key4",
			expectedKeys: map[string]struct{}{"key2": {}, "key3": {}, "key4": {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw = NewKeyedWindow(time.Unix(60, 0), time.Unix(120, 0))
			for k := range tt.given.slots {
				kw.AddSlot(k)
			}
			kw.AddSlot(tt.input)
			assert.Equal(t, len(tt.expectedKeys), len(kw.slots))
			for k := range tt.expectedKeys {
				_, ok := kw.slots[k]
				assert.True(t, ok)
			}
		})
	}
}

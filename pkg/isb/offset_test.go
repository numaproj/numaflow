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

package isb

import (
	"reflect"
	"testing"
)

func TestDeduplicateOffsets(t *testing.T) {
	tests := []struct {
		name         string
		input        []Offset
		expectOutput []Offset
	}{
		{
			name: "deduplicate_using_SimpleStringOffset",
			input: []Offset{
				SimpleStringOffset(func() string { return "simple-offset-1" }),
				SimpleStringOffset(func() string { return "simple-offset-2" }),
				SimpleStringOffset(func() string { return "simple-offset-2" }),
				SimpleStringOffset(func() string { return "simple-offset-3" }),
			},
			expectOutput: []Offset{
				SimpleStringOffset(func() string { return "simple-offset-1" }),
				SimpleStringOffset(func() string { return "simple-offset-2" }),
				SimpleStringOffset(func() string { return "simple-offset-3" }),
			},
		},
		{
			name: "deduplicate_using_SimpleStringOffset_no_duplicate",
			input: []Offset{
				SimpleStringOffset(func() string { return "simple-offset-1" }),
				SimpleStringOffset(func() string { return "simple-offset-2" }),
			},
			expectOutput: []Offset{
				SimpleStringOffset(func() string { return "simple-offset-1" }),
				SimpleStringOffset(func() string { return "simple-offset-2" }),
			},
		},
		{
			name: "deduplicate_using_SimpleIntOffset",
			input: []Offset{
				SimpleIntOffset(func() int64 { return int64(1) }),
				SimpleIntOffset(func() int64 { return int64(2) }),
				SimpleIntOffset(func() int64 { return int64(2) }),
				SimpleIntOffset(func() int64 { return int64(3) }),
			},
			expectOutput: []Offset{
				SimpleIntOffset(func() int64 { return int64(1) }),
				SimpleIntOffset(func() int64 { return int64(2) }),
				SimpleIntOffset(func() int64 { return int64(3) }),
			},
		},
		{
			name: "deduplicate_using_SimpleIntOffset_no_duplicate",
			input: []Offset{
				SimpleIntOffset(func() int64 { return int64(1) }),
				SimpleIntOffset(func() int64 { return int64(2) }),
			},
			expectOutput: []Offset{
				SimpleIntOffset(func() int64 { return int64(1) }),
				SimpleIntOffset(func() int64 { return int64(2) }),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeduplicateOffsets(tt.input)
			reflect.DeepEqual(got, tt.expectOutput)
		})
	}
}

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

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUniqStrList(t *testing.T) {
	l := NewUniqueStringList()
	assert.Equal(t, 0, l.Length())
	assert.Equal(t, "", l.Front())

	// Add a new string
	l.PushBack("a")
	assert.Equal(t, 1, l.Length())
	assert.Equal(t, "a", l.Front())
	assert.Equal(t, true, l.Contains("a"))
	assert.Equal(t, false, l.Contains("b"))

	// Add a new string
	l.PushBack("b")
	assert.Equal(t, 2, l.Length())
	assert.Equal(t, "a", l.Front())
	assert.Equal(t, true, l.Contains("a"))
	assert.Equal(t, true, l.Contains("b"))

	// Add a new string
	l.PushBack("c")
	assert.Equal(t, 3, l.Length())
	assert.Equal(t, "a", l.Front())

	// Move "a" to back of the list
	l.MoveToBack("a")
	assert.Equal(t, 3, l.Length())
	assert.Equal(t, "b", l.Front())

	// Remove "b"
	l.Remove("b")
	assert.Equal(t, 2, l.Length())
	assert.Equal(t, "c", l.Front())
	assert.Equal(t, false, l.Contains("b"))
	assert.Equal(t, true, l.Contains("a"))
	assert.Equal(t, true, l.Contains("c"))

	// Operate on a non-existing string
	l.MoveToBack("non-existing")
	assert.Equal(t, 2, l.Length())
	assert.Equal(t, "c", l.Front())
	assert.Equal(t, false, l.Contains("non-existing"))
	assert.Equal(t, true, l.Contains("a"))
	assert.Equal(t, true, l.Contains("c"))
}

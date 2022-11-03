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

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppend(t *testing.T) {
	q := New[int](2)
	q.Append(1)
	q.Append(2)
	assert.Equal(t, 2, q.Length())
	assert.Contains(t, q.Items(), 1)
	assert.Contains(t, q.Items(), 2)
	q.Append(3)
	assert.Equal(t, 2, q.Length())
	assert.Contains(t, q.Items(), 2)
	assert.Contains(t, q.Items(), 3)
	q.Append(4)
	q.Append(5)
	q.Append(6)
	assert.Equal(t, 5, q.Items()[0])
	assert.Equal(t, 6, q.Items()[1])
	assert.Equal(t, 6, q.ReversedItems()[0])
	assert.Equal(t, 5, q.ReversedItems()[1])
}

func TestReverse(t *testing.T) {
	l := []int{1, 2, 3}
	l1 := reverse(l)
	assert.Equal(t, 3, len(l1))
	assert.Equal(t, 3, l1[0])
	assert.Equal(t, 2, l1[1])
	assert.Equal(t, 1, l1[2])
}

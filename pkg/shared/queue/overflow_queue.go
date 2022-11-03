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

import "sync"

// OverflowQeueue is a thread safe queue implementation with max size, and the oldest elements automatically overflow.
type OverflowQueue[T any] struct {
	elements []T
	maxSize  int
	lock     *sync.RWMutex
}

func New[T any](size int) *OverflowQueue[T] {
	return &OverflowQueue[T]{
		elements: []T{},
		maxSize:  size,
		lock:     new(sync.RWMutex),
	}
}

// Append adds an element to the queue
func (q *OverflowQueue[T]) Append(value T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.elements) >= q.maxSize {
		q.elements = q.elements[1:]
	}
	q.elements = append(q.elements, value)
}

// Items returns a copy of the elements in the queue
func (q *OverflowQueue[T]) Items() []T {
	q.lock.RLock()
	defer q.lock.RUnlock()
	r := make([]T, len(q.elements))
	_ = copy(r, q.elements)
	return r
}

// ReversedItems returns a reversed copy of the elements in the queue
func (q *OverflowQueue[T]) ReversedItems() []T {
	return reverse(q.Items())
}

// Length returns the current length of the queue
func (q *OverflowQueue[T]) Length() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.elements)
}

func reverse[T any](input []T) []T {
	if len(input) == 0 {
		return input
	}
	return append(reverse(input[1:]), input[0])
}

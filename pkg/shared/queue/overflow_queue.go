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

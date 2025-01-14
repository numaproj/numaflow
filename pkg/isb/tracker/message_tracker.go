package tracker

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

// MessageTracker is used to store a key value pair for string and *ReadMessage
// as it can be accessed by concurrent goroutines, we keep all operations
// under a mutex
type MessageTracker struct {
	lock sync.RWMutex
	m    map[string]*isb.ReadMessage
}

// NewMessageTracker initializes a new instance of a Tracker
func NewMessageTracker(messages []*isb.ReadMessage) *MessageTracker {
	m := make(map[string]*isb.ReadMessage, len(messages))
	for _, msg := range messages {
		id := msg.ReadOffset.String()
		m[id] = msg
	}
	return &MessageTracker{
		m:    m,
		lock: sync.RWMutex{},
	}
}

// Remove will remove the entry for a given id and return the stored value corresponding to this id.
// A `nil` return value indicates that the id doesn't exist in the tracker.
func (t *MessageTracker) Remove(id string) *isb.ReadMessage {
	t.lock.Lock()
	defer t.lock.Unlock()
	item, ok := t.m[id]
	if !ok {
		return nil
	}
	delete(t.m, id)
	return item
}

// IsEmpty is a helper function which checks if the Tracker map is empty
// return true if empty
func (t *MessageTracker) IsEmpty() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return len(t.m) == 0
}

// Len returns the number of messages currently stored in the tracker
func (t *MessageTracker) Len() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return len(t.m)
}

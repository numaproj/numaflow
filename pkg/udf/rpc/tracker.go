package rpc

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

// tracker is used to store a key value pair for string and *isb.ReadMessage
// as it can be accessed by concurrent goroutines, we keep all operations
// under a mutex
type tracker struct {
	lock sync.RWMutex
	m    map[string]*isb.ReadMessage
}

// NewTracker initializes a new instance of a tracker
func NewTracker() *tracker {
	return &tracker{
		m:    make(map[string]*isb.ReadMessage),
		lock: sync.RWMutex{},
	}
}

// addRequest add a new entry for a given message to the tracker.
// the key is chosen as the read offset of the message
func (t *tracker) addRequest(msg *isb.ReadMessage) {
	id := msg.ReadOffset.String()
	t.set(id, msg)
}

// getRequest returns the message corresponding to a given id, along with a bool
// to indicate if it does not exist
func (t *tracker) getRequest(id string) (*isb.ReadMessage, bool) {
	return t.get(id)
}

// removeRequest will remove the entry for a given id
func (t *tracker) removeRequest(id string) {
	t.delete(id)
}

// get is a helper function which fetches the message corresponding to a given id
// it acquires a lock before accessing the map
func (t *tracker) get(key string) (*isb.ReadMessage, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	item, ok := t.m[key]
	return item, ok
}

// set is a helper function which add a key, value pair to the tracker map
// it acquires a lock before accessing the map
func (t *tracker) set(key string, msg *isb.ReadMessage) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m[key] = msg
}

// delete is a helper function which will remove the entry for a given id
// it acquires a lock before accessing the map
func (t *tracker) delete(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.m, key)
}

// isEmpty is a helper function which checks if the tracker map is empty
// return true if empty
func (t *tracker) isEmpty() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	items := len(t.m)
	return items == 0
}

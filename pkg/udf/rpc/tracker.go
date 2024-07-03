package rpc

import (
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

type tracker struct {
	lock sync.RWMutex
	m    map[string]*isb.ReadMessage
}

func NewTracker() *tracker {
	return &tracker{
		m: make(map[string]*isb.ReadMessage),
	}
}

func (t *tracker) addRequest(msg *isb.ReadMessage) {
	id := msg.ReadOffset.String()
	t.set(id, msg)
}

func (t *tracker) getRequest(id string) (*isb.ReadMessage, bool) {
	return t.get(id)
}

func (t *tracker) removeRequest(id string) {
	t.delete(id)
}

func (t *tracker) get(key string) (*isb.ReadMessage, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	item, ok := t.m[key]
	return item, ok
}

func (t *tracker) set(key string, msg *isb.ReadMessage) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m[key] = msg
}

func (t *tracker) delete(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.m, key)
}

func (t *tracker) getItems() []*isb.ReadMessage {
	t.lock.Lock()
	defer t.lock.Unlock()
	items := make([]*isb.ReadMessage, 0, len(t.m))
	for _, vals := range t.m {
		items = append(items, vals)
	}
	return items
}

func (t *tracker) clear() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m = make(map[string]*isb.ReadMessage)
}

package rpc

import (
	"log"
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Tracker struct {
	lock sync.RWMutex
	m    map[string]*isb.ReadMessage
}

func NewTracker() *Tracker {
	return &Tracker{
		m: make(map[string]*isb.ReadMessage),
	}
}

func (t *Tracker) AddRequest(msg *isb.ReadMessage) {
	id := msg.ReadOffset.String()
	t.Set(id, msg)
}

func (t *Tracker) GetRequest(id string) (*isb.ReadMessage, bool) {
	return t.Get(id)
}

func (t *Tracker) RemoveRequest(id string) {
	t.Delete(id)
}

func (t *Tracker) PrintAll() {
	t.lock.RLock()
	defer t.lock.RUnlock()
	for k, v := range t.m {
		log.Println("MYDEBUG: MAP VALS", k, " ", v)
	}
}

func (t *Tracker) Get(key string) (*isb.ReadMessage, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	item, ok := t.m[key]
	return item, ok
}

func (t *Tracker) Set(key string, msg *isb.ReadMessage) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m[key] = msg
}

func (t *Tracker) Delete(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.m, key)
}

func (t *Tracker) GetItems() []*isb.ReadMessage {
	t.lock.Lock()
	defer t.lock.Unlock()
	items := make([]*isb.ReadMessage, 0, len(t.m))
	for _, vals := range t.m {
		items = append(items, vals)
	}
	return items
}

func (t *Tracker) Clear() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m = make(map[string]*isb.ReadMessage)
}

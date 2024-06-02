package tracker

import (
	"log"
	"sync"

	"github.com/google/uuid"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Tracker struct {
	requestMap sync.Map
	// TODO(stream): check if this will be inefficient, and there is a better way for this
	responseIdx sync.Map
	lock        sync.RWMutex
	m           map[string]*isb.ReadMessage
}

func NewTracker() *Tracker {
	return &Tracker{
		requestMap:  sync.Map{},
		responseIdx: sync.Map{},
		m:           make(map[string]*isb.ReadMessage),
	}
}

func GetNewId() string {
	id, _ := uuid.NewUUID()
	return id.String()
}

func (t *Tracker) AddRequest(msg *isb.ReadMessage) string {
	id := GetNewId()
	//t.requestMap.Store(id, msg)
	t.Set(id, msg)
	return id
}

func (t *Tracker) GetRequest(id string) (*isb.ReadMessage, bool) {
	return t.Get(id)
}

func (t *Tracker) NewResponse(id string) {
	t.responseIdx.Store(id, 1)
}

func (t *Tracker) IncrementRespIdx(id string) bool {
	idx, ok := t.responseIdx.Load(id)
	if !ok {
		return ok
	}
	newIdx := idx.(int) + 1
	t.responseIdx.Store(id, newIdx)
	return true
}

func (t *Tracker) GetIdx(id string) (int, bool) {
	idx, ok := t.responseIdx.Load(id)
	if !ok {
		return -1, ok
	}
	return idx.(int), ok
}

func (t *Tracker) RemoveRequest(id string) {
	//t.requestMap.Delete(id)
	//t.responseIdx.Delete(id)
	t.Delete(id)
}

func (t *Tracker) PrintAll() {
	t.lock.RLock()
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

func (t *Tracker) Set(key string, value *isb.ReadMessage) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m[key] = value
}

func (t *Tracker) Delete(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.m, key)
}

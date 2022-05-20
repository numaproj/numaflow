package fetch

import "sync"

// ProcessorHeartbeat has details about each processor heartbeat. This information is populated
// by watching the Vn-1th vertex's processors. It stores only the latest heartbeat value.
type ProcessorHeartbeat struct {
	// heartbeat has the processor name to last heartbeat timestamp
	heartbeat map[string]int64
	lock      sync.RWMutex
}

// NewProcessorHeartbeat returns ProcessorHeartbeat.
func NewProcessorHeartbeat() *ProcessorHeartbeat {
	return &ProcessorHeartbeat{
		heartbeat: make(map[string]int64),
	}
}

// Put inserts a heartbeat entry for a given processor key and value.
func (hb *ProcessorHeartbeat) Put(key string, value int64) {
	if value == -1 {
		return
	}
	hb.lock.Lock()
	defer hb.lock.Unlock()
	hb.heartbeat[key] = value
}

// Get gets the heartbeat for a given processor.
func (hb *ProcessorHeartbeat) Get(key string) int64 {
	hb.lock.RLock()
	defer hb.lock.RUnlock()
	if value, ok := hb.heartbeat[key]; ok {
		return value
	}
	return -1
}

// GetAll returns all the heartbeat entries in the heartbeat table.
func (hb *ProcessorHeartbeat) GetAll() map[string]int64 {
	hb.lock.RLock()
	defer hb.lock.RUnlock()
	var all = make(map[string]int64, len(hb.heartbeat))
	for k, v := range hb.heartbeat {
		all[k] = v
	}
	return all
}

// Delete deletes a processor from the ProcessorHeartbeat table.
func (hb *ProcessorHeartbeat) Delete(key string) {
	hb.lock.Lock()
	defer hb.lock.Unlock()
	delete(hb.heartbeat, key)
}

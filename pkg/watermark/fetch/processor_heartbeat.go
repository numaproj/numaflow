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

package fetch

import "sync"

// processorHeartbeat has details about each processor heartbeat. This information is populated
// by watching the Vn-1th vertex's processors. It stores only the latest heartbeat value.
type processorHeartbeat struct {
	// heartbeat has the processor name to last heartbeat timestamp
	heartbeat map[string]int64
	lock      sync.RWMutex
}

// newProcessorHeartbeat returns processorHeartbeat.
func newProcessorHeartbeat() *processorHeartbeat {
	return &processorHeartbeat{
		heartbeat: make(map[string]int64),
	}
}

// put inserts a heartbeat entry for a given processor key and value.
func (hb *processorHeartbeat) put(key string, value int64) {
	if value == -1 {
		return
	}
	hb.lock.Lock()
	defer hb.lock.Unlock()
	hb.heartbeat[key] = value
}

// get the heartbeat for a given processor.
func (hb *processorHeartbeat) get(key string) int64 {
	hb.lock.RLock()
	defer hb.lock.RUnlock()
	if value, ok := hb.heartbeat[key]; ok {
		return value
	}
	return -1
}

// getAll returns all the heartbeat entries in the heartbeat table.
func (hb *processorHeartbeat) getAll() map[string]int64 {
	hb.lock.RLock()
	defer hb.lock.RUnlock()
	var all = make(map[string]int64, len(hb.heartbeat))
	for k, v := range hb.heartbeat {
		all[k] = v
	}
	return all
}

// delete deletes a processor from the processorHeartbeat table.
func (hb *processorHeartbeat) delete(key string) {
	hb.lock.Lock()
	defer hb.lock.Unlock()
	delete(hb.heartbeat, key)
}

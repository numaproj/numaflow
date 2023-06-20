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

package server

import (
	"container/list"
	"sync"
)

// UniqueStringList is a list of strings that only allows unique values.
// the underlying list is a doubly linked list.
type UniqueStringList struct {
	l    *list.List
	m    map[string]*list.Element
	lock *sync.RWMutex
}

func NewUniqueStringList() *UniqueStringList {
	return &UniqueStringList{
		l:    list.New(),
		m:    make(map[string]*list.Element),
		lock: new(sync.RWMutex),
	}
}

// PushBack adds a value to the back of the list, if the value doesn't exist in the list.
func (l *UniqueStringList) PushBack(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, ok := l.m[value]; !ok {
		l.m[value] = l.l.PushBack(value)
	}
}

// MoveToBack moves the element to the back of the list, if the value exists in the list.
func (l *UniqueStringList) MoveToBack(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if e, ok := l.m[value]; ok {
		l.l.MoveToBack(e)
	}
}

// Front returns the first string of list l or empty string if the list is empty.
func (l *UniqueStringList) Front() string {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.Length() == 0 {
		return ""
	}
	return l.l.Front().Value.(string)
}

// Length returns the number of elements of list l.
func (l *UniqueStringList) Length() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.l.Len()
}

// Contains returns true if the value exists in the list.
func (l *UniqueStringList) Contains(value string) bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	_, ok := l.m[value]
	return ok
}

// Remove removes the string from the list, if it exists in the list.
func (l *UniqueStringList) Remove(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if e, ok := l.m[value]; ok {
		l.l.Remove(e)
		delete(l.m, value)
	}
}

// ToString returns a comma separated string of the list values.
func (l *UniqueStringList) ToString() string {
	l.lock.RLock()
	defer l.lock.RUnlock()
	var s string
	for e := l.l.Front(); e != nil; e = e.Next() {
		s += e.Value.(string) + ","
	}
	return s
}

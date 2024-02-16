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

package memory

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned"

	"go.uber.org/zap"
)

// memoryStore implements PBQStore which stores the data in memory
type memoryStore struct {
	closed      bool
	writePos    int64
	readPos     int64
	storage     []*isb.ReadMessage
	storeSize   int64
	log         *zap.SugaredLogger
	partitionID partition.ID
}

// Replay will replay all the messages persisted in store
// this function will be invoked during bootstrap if there is a restart
func (m *memoryStore) Replay() (<-chan *isb.ReadMessage, <-chan error) {
	msgChan := make(chan *isb.ReadMessage)
	errChan := make(chan error)
	go func() {
		for _, msg := range m.storage {
			msgChan <- msg
		}
		close(msgChan)
		close(errChan)
	}()
	return msgChan, errChan
}

// WriteToStore writes a message to store
func (m *memoryStore) Write(msg *isb.ReadMessage) error {
	if m.writePos >= m.storeSize {
		m.log.Errorw(aligned.ErrWriteStoreFull.Error(), zap.Any("msg header", msg.Header))
		return aligned.ErrWriteStoreFull
	}
	if m.closed {
		m.log.Errorw(aligned.ErrWriteStoreClosed.Error(), zap.Any("msg header", msg.Header))
		return aligned.ErrWriteStoreClosed
	}
	m.storage[m.writePos] = msg
	m.writePos += 1
	return nil
}

// Close closes the store, no more writes to persistent store
// no implementation for in memory store
func (m *memoryStore) Close() error {
	m.closed = true
	return nil
}

func (m *memoryStore) PartitionID() partition.ID {
	return m.partitionID
}

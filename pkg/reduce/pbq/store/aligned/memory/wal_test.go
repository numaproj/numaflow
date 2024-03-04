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
	"context"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"

	"github.com/stretchr/testify/assert"
)

func TestMemoryStore_WriteToStore(t *testing.T) {
	ctx := context.Background()

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "new-partition",
	}
	// create a store of size 100 (it can store max 100 messages)
	memStore, err := NewMemoryStores(WithStoreSize(100)).CreateStore(ctx, partitionID)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestReadMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}
}

func TestMemoryStore_ReadFromStore(t *testing.T) {
	ctx := context.Background()

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "new-partition",
	}

	// create a store of size 100 (it can store max 100 messages)
	memStore, err := NewMemoryStores(WithStoreSize(100)).CreateStore(ctx, partitionID)
	assert.NoError(t, err)

	//write 10 isb messages to persisted store
	msgCount := 10
	startTime := time.Now()
	writeMessages := testutils.BuildTestReadMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}
	msgCh, errCh := memStore.Replay()
	readMessages := make([]*isb.ReadMessage, 0)
outerLoop:
	for {
		select {
		case msg, ok := <-msgCh:
			if msg != nil {
				readMessages = append(readMessages, msg)
			}
			if !ok {
				break outerLoop
			}
		case err, ok := <-errCh:
			if !ok {
				break outerLoop
			}
			assert.NoError(t, err)
			break outerLoop
		}
	}
	// number of read messages should be equal to msgCount
	assert.Len(t, readMessages, msgCount)
}

func TestEmptyStore_Read(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := int64(100)
	ctx := context.Background()

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "new-partition",
	}

	memStore, err := NewMemoryStores(WithStoreSize(storeSize)).CreateStore(ctx, partitionID)
	assert.NoError(t, err)
	msgCh, errCh := memStore.Replay()
	readMessages := make([]*isb.ReadMessage, 0)
outerLoop:
	for {
		select {
		case msg, ok := <-msgCh:
			if msg != nil {
				readMessages = append(readMessages, msg)
			}
			if !ok {
				break outerLoop
			}
		case err, ok := <-errCh:
			if !ok {
				break outerLoop
			}
			assert.NoError(t, err)
			break outerLoop
		}
	}
	// since store is empty, there should not be any messages to read
	assert.Len(t, readMessages, 0)

}

func TestFullStore_Write(t *testing.T) {
	// create a store of size 100 (it can store max 100 messages)
	storeSize := int64(100)
	ctx := context.Background()

	partitionID := partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "new-partition",
	}

	memStore, err := NewMemoryStores(WithStoreSize(storeSize)).CreateStore(ctx, partitionID)
	assert.NoError(t, err)

	//write 100 isb messages to persisted store
	msgCount := 100
	startTime := time.Now()
	writeMessages := testutils.BuildTestReadMessages(int64(msgCount), startTime)

	for _, msg := range writeMessages {
		err := memStore.Write(&msg)
		assert.NoError(t, err)
	}

	// now the store is full, if we write to store we should get an error
	err = memStore.Write(&writeMessages[0])
	assert.ErrorContains(t, err, "store is full")
}

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

package fs

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

var vi = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}},
	Hostname: "test-host",
	Replica:  0,
}

func Test_writeReadHeader(t *testing.T) {
	id := partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Slot:  "test1",
	}

	tmp := t.TempDir()
	stores := NewFSManager(vi, WithStorePath(tmp))
	store, err := stores.CreateWAL(context.Background(), id)
	assert.NoError(t, err)
	wal := store.(*alignedWAL)
	fName := wal.fp.Name()
	assert.NoError(t, err)
	// read will fail because the file was opened only in write only mode
	_, err = wal.readWALHeader()
	assert.Error(t, err)
	err = wal.Close()
	fmt.Println(fName)
	assert.NoError(t, err)

	openWAL, err := NewAlignedWriteOnlyWAL(&id, fName, dfv1.DefaultWALMaxSyncSize, dfv1.DefaultWALSyncDuration, "testPipeline", "testVertex", 0)
	assert.NoError(t, err)
	// we have already read the header in OpenWAL
	_, err = openWAL.(*alignedWAL).readWALHeader()
	assert.Error(t, err)

	// compare the original ID with read ID
	assert.Equal(t, id, *openWAL.(*alignedWAL).partitionID)

	err = openWAL.Close()
	assert.NoError(t, err)
}

func Test_encodeDecodeHeader(t *testing.T) {
	tests := []struct {
		name    string
		id      *partition.ID
		want    *bytes.Buffer
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "enc_dec_good",
			wantErr: assert.NoError,
			id: &partition.ID{
				Start: time.Unix(1665109020, 0).In(location),
				End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
				Slot:  "test1,test2",
			},
		},
		{
			name:    "enc_dec_nodata",
			wantErr: assert.NoError,
			id: &partition.ID{
				Start: time.Time{},
				End:   time.Time{},
				Slot:  "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			stores := NewFSManager(vi, WithStorePath(tmp))
			wal, err := stores.CreateWAL(context.Background(), *tt.id)
			assert.NoError(t, err)
			newWal := wal.(*alignedWAL)
			got, err := newWal.encodeWALHeader(tt.id)
			if !tt.wantErr(t, err, fmt.Sprintf("encodeWALHeader(%v)", tt.id)) {
				return
			}
			result, err := decodeWALHeader(got)
			assert.NoError(t, err)
			assert.Equalf(t, tt.id, result, "encodeWALHeader(%v)", tt.id)
			err = newWal.Close()
			assert.NoError(t, err)
		})
	}
}

func Test_writeReadEntry(t *testing.T) {
	id := partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Slot:  "test1",
	}

	tmp := t.TempDir()
	stores := NewFSManager(vi, WithStorePath(tmp))
	wal, err := stores.CreateWAL(context.Background(), id)
	assert.NoError(t, err)

	startTime := time.Unix(1665109020, 0).In(location)
	msgCount := 2
	writeMessages := testutils.BuildTestReadMessagesIntOffset(int64(msgCount), startTime, nil)
	message := writeMessages[0]
	err = wal.Write(&message)
	assert.NoError(t, err)
	err = wal.Close()
	assert.NoError(t, err)

	// Reopen the alignedWAL for read and write.
	discoveredStores, err := stores.DiscoverWALs(context.Background())
	assert.Len(t, discoveredStores, 1)
	assert.NoError(t, err)
	store := discoveredStores[0]
	newWal := store.(*alignedWAL)
	// we have already read the header in OpenWAL
	_, err = newWal.readWALHeader()
	assert.Error(t, err)

	msgCh, errCh := newWal.Replay()
	actualMessages := make([]*isb.ReadMessage, 0)
outerLoop:
	for {
		select {
		case msg, ok := <-msgCh:
			if msg != nil {
				actualMessages = append(actualMessages, msg)
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
	// Check we reach the end of file
	assert.Equal(t, newWal.readUpTo, newWal.rOffset)

	assert.Len(t, actualMessages, 1)
	actualMessage := actualMessages[0]
	assert.Equalf(t, message.Message, actualMessage.Message, "Read(%v)", message.Message)
	expectedOffset, err := message.ReadOffset.Sequence()
	assert.NoError(t, err)
	actualOffset, err := actualMessage.ReadOffset.Sequence()
	assert.NoError(t, err)
	assert.Equalf(t, expectedOffset, actualOffset, "Read(%v)", message.ReadOffset)
	assert.Equalf(t, message.Watermark, actualMessage.Watermark, "encodeWALMessage(%v)", message.Watermark)

	// Start to write an entry again
	err = newWal.Write(&message)
	assert.NoError(t, err)
	err = newWal.Close()
	assert.NoError(t, err)
}

func Test_encodeDecodeEntry(t *testing.T) {
	// write 1 isb messages to persisted store
	startTime := time.Unix(1665109020, 0).In(location)
	writeMessages := testutils.BuildTestReadMessagesIntOffset(1, startTime, nil)
	firstMessage := writeMessages[0]
	tests := []struct {
		name    string
		message *isb.ReadMessage
		want    *bytes.Buffer
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "enc_dec_good",
			wantErr: assert.NoError,
			message: &firstMessage,
		},
		{
			name:    "enc_dec_nodata",
			wantErr: assert.NoError,
			message: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{},
					Body:   isb.Body{},
				},
				ReadOffset: isb.SimpleIntOffset(func() int64 { return int64(2) }),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			stores := NewFSManager(vi, WithStorePath(tmp))
			wal, err := stores.CreateWAL(context.Background(), partition.ID{})
			assert.NoError(t, err)
			newWal := wal.(*alignedWAL)

			got, err := newWal.encodeWALMessage(tt.message)
			if !tt.wantErr(t, err, fmt.Sprintf("encodeWALMessage(%v)", tt.message)) {
				return
			}

			result, _, err := decodeReadMessage(bytes.NewReader(got.Bytes()))
			assert.NoError(t, err)
			assert.Equalf(t, tt.message.Message, result.Message, "encodeWALMessage(%v)", tt.message.Message)
			expectedOffset, err := tt.message.ReadOffset.Sequence()
			assert.NoError(t, err)
			actualOffset, err := result.ReadOffset.Sequence()
			assert.NoError(t, err)
			assert.Equalf(t, expectedOffset, actualOffset, "encodeWALMessage(%v)", tt.message.ReadOffset)
			assert.Equalf(t, tt.message.Watermark, result.Watermark, "encodeWALMessage(%v)", tt.message.Watermark)
			err = newWal.Close()
			assert.NoError(t, err)
		})
	}
}

func Test_batchSyncWithMaxBatchSize(t *testing.T) {
	ctx := context.Background()
	id := partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Slot:  "test1",
	}

	tmp := t.TempDir()
	stores := NewFSManager(vi, WithStorePath(tmp))
	wal, err := stores.CreateWAL(context.Background(), id)
	assert.NoError(t, err)

	tempWAL := wal.(*alignedWAL)
	tempWAL.prevSyncedTime = time.Now()

	startTime := time.Unix(1665109020, 0).In(location)
	msgCount := 2
	writeMessages := testutils.BuildTestReadMessagesIntOffset(int64(msgCount), startTime, nil)
	message := writeMessages[0]
	err = wal.Write(&message)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), tempWAL.prevSyncedWOffset)
	assert.NoError(t, err)

	tempWAL.maxBatchSize = 10
	assert.NoError(t, err)
	err = wal.Write(&message)
	assert.NoError(t, err)
	assert.Equal(t, int64(308), tempWAL.prevSyncedWOffset)

	err = wal.Close()
	assert.NoError(t, err)

	// Reopen the alignedWAL for read and write.
	discoveredStores, err := stores.DiscoverWALs(ctx)
	assert.Len(t, discoveredStores, 1)
	assert.NoError(t, err)
	newWal := discoveredStores[0].(*alignedWAL)
	// we have already read the header in OpenWAL
	_, err = newWal.readWALHeader()
	assert.Error(t, err)

	msgCh, errCh := newWal.Replay()
	actualMessages := make([]*isb.ReadMessage, 0)
outerLoop:
	for {
		select {
		case msg, ok := <-msgCh:
			if msg != nil {
				actualMessages = append(actualMessages, msg)
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

	// Check we reach the end of file
	assert.Equal(t, newWal.readUpTo, newWal.rOffset)

	assert.Len(t, actualMessages, 2)
	actualMessage := actualMessages[0]
	assert.Equalf(t, message.Message, actualMessage.Message, "Read(%v)", message.Message)
	expectedOffset, err := message.ReadOffset.Sequence()
	assert.NoError(t, err)
	actualOffset, err := actualMessage.ReadOffset.Sequence()
	assert.NoError(t, err)
	assert.Equalf(t, expectedOffset, actualOffset, "Read(%v)", message.ReadOffset)
	assert.Equalf(t, message.Watermark, actualMessage.Watermark, "encodeWALMessage(%v)", message.Watermark)

	// Start to write an entry again
	err = newWal.Write(&message)
	assert.NoError(t, err)
	err = newWal.Close()
	assert.NoError(t, err)
}

func Test_batchSyncWithSyncDuration(t *testing.T) {
	id := partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Slot:  "test1",
	}

	tmp := t.TempDir()
	stores := &fsManager{
		storePath:    tmp,
		maxBatchSize: dfv1.DefaultWALMaxSyncSize,
		syncDuration: 0,
		pipelineName: vi.Vertex.Spec.PipelineName,
		vertexName:   vi.Vertex.Spec.AbstractVertex.Name,
		replicaIndex: vi.Replica,
		activeWals:   make(map[string]wal.WAL),
	}

	wal, err := stores.CreateWAL(context.Background(), id)
	assert.NoError(t, err)

	tempWAL := wal.(*alignedWAL)

	startTime := time.Unix(1665109020, 0).In(location)
	msgCount := 2
	writeMessages := testutils.BuildTestReadMessagesIntOffset(int64(msgCount), startTime, nil)
	message := writeMessages[0]
	storePrevSyncedTime := tempWAL.prevSyncedTime
	err = wal.Write(&message)
	assert.Equal(t, int64(173), tempWAL.prevSyncedWOffset)
	assert.NotEqual(t, storePrevSyncedTime, tempWAL.prevSyncedTime)
	assert.NoError(t, err)

	storePrevSyncedTime = tempWAL.prevSyncedTime
	tempWAL.syncDuration = 10 * time.Second
	assert.NoError(t, err)
	err = wal.Write(&message)
	assert.NoError(t, err)
	assert.Equal(t, tempWAL.prevSyncedTime, storePrevSyncedTime)

	err = wal.Close()
	assert.NoError(t, err)

	// Reopen the alignedWAL for read and write.
	discoverStores, err := stores.DiscoverWALs(context.Background())
	assert.Len(t, discoverStores, 1)
	assert.NoError(t, err)
	newWal := discoverStores[0].(*alignedWAL)
	// we have already read the header in OpenWAL
	_, err = newWal.readWALHeader()
	assert.Error(t, err)

	msgCh, errCh := newWal.Replay()
	actualMessages := make([]*isb.ReadMessage, 0)
outerLoop:
	for {
		select {
		case msg, ok := <-msgCh:
			if msg != nil {
				actualMessages = append(actualMessages, msg)
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
	// Check we reach the end of file
	assert.Equal(t, newWal.readUpTo, newWal.rOffset)

	assert.Len(t, actualMessages, 2)
	actualMessage := actualMessages[0]
	assert.Equalf(t, message.Message, actualMessage.Message, "Read(%v)", message.Message)
	expectedOffset, err := message.ReadOffset.Sequence()
	assert.NoError(t, err)
	actualOffset, err := actualMessage.ReadOffset.Sequence()
	assert.NoError(t, err)
	assert.Equalf(t, expectedOffset, actualOffset, "Read(%v)", message.ReadOffset)
	assert.Equalf(t, message.Watermark, actualMessage.Watermark, "encodeWALMessage(%v)", message.Watermark)

	// Start to write an entry again
	err = newWal.Write(&message)
	assert.NoError(t, err)
	err = newWal.Close()
	assert.NoError(t, err)
}

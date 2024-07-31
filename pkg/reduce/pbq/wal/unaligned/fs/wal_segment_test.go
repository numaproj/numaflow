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
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/window"
)

var vertexInstance = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					Kafka: &dfv1.KafkaSink{},
				},
			},
		},
	}},
	Replica: 0,
}

func TestUnalignedWAL_Write(t *testing.T) {

	ctx := context.Background()
	segmentDir := t.TempDir()
	defer func(path string) {
		cleanupDir(path)
	}(segmentDir)

	compactDir := t.TempDir()
	defer func(path string) {
		cleanupDir(path)
	}(compactDir)

	partitionId := window.SharedUnalignedPartition
	s, err := NewUnalignedWriteOnlyWAL(ctx, "test-pl", "test-vtx", 0, &partitionId, WithStoreOptions(segmentDir, compactDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(100, time.UnixMilli(60000), nil)

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	// close the unalignedWAL
	err = s.Close()
	assert.NoError(t, err)

	// list all the filesToReplay in the directory
	files, err := os.ReadDir(segmentDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)
}

func TestUnalignedWAL_Replay(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	defer func(path string) {
		cleanupDir(path)
	}(tempDir)

	compactDir := t.TempDir()
	defer func(path string) {
		cleanupDir(path)
	}(compactDir)

	partitionId := window.SharedUnalignedPartition
	s, err := NewUnalignedWriteOnlyWAL(ctx, "test-pl", "test-vtx", 0, &partitionId, WithStoreOptions(tempDir, compactDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(1000, time.UnixMilli(60000), []string{"key-1", "key-2"})

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	// close the unalignedWAL
	err = s.Close()
	assert.NoError(t, err)

	wm := NewFSManager(ctx, tempDir, compactDir, vertexInstance)
	wls, err := wm.DiscoverWALs(ctx)
	assert.NoError(t, err)

	// replay the messages
	readCh, errCh := wls[0].Replay()
	replayedMessages := make([]*isb.ReadMessage, 0)
readLoop:
	for {
		select {
		case msg, ok := <-readCh:
			if !ok {
				break readLoop
			}
			replayedMessages = append(replayedMessages, msg)
		case err := <-errCh:
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, err)
	assert.Equal(t, len(readMessages), len(replayedMessages))

	// order is important
	for i := 0; i < len(readMessages); i++ {
		assert.Equal(t, readMessages[i].EventTime.UnixMilli(), replayedMessages[i].EventTime.UnixMilli())
	}
}

func WithStoreOptions(segmentPath string, compactPath string) WALOption {
	return func(s *unalignedWAL) {
		s.segmentWALPath = segmentPath
		s.compactWALPath = compactPath
		s.segmentSize = 1024 * 1024 * 10
		s.syncDuration = 10 * time.Second
		s.maxBatchSize = 1024 * 50
		s.segmentRotationDuration = 120 * time.Second
	}
}

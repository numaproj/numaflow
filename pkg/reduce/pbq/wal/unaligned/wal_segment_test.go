package unaligned

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestUnalignedWAL_Write(t *testing.T) {

	tempDir := t.TempDir()
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	partitionId := window.SharedUnalignedPartition
	s, err := NewUnalignedWriteOnlyWAL(&partitionId, WithStoreOptions(tempDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(100, time.UnixMilli(60000))

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	// close the unalignedWAL
	err = s.Close()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err := os.ReadDir(tempDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)
}

func TestUnalignedWAL_Replay(t *testing.T) {
	tempDir := t.TempDir()
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	partitionId := window.SharedUnalignedPartition
	s, err := NewUnalignedWriteOnlyWAL(&partitionId, WithStoreOptions(tempDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(100, time.UnixMilli(60000))

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	// close the unalignedWAL
	err = s.Close()
	assert.NoError(t, err)

	// create a new unalignedWAL
	s, err = NewUnalignedReadWriteWAL(WithStoreOptions(tempDir))
	assert.NoError(t, err)

	// replay the messages
	readCh, errCh := s.Replay()
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

func WithStoreOptions(path string) WALOption {
	return func(s *unalignedWAL) {
		s.storeDataPath = path
		s.segmentSize = 1024
		s.syncDuration = time.Second
		s.maxBatchSize = 1024 * 50
		s.segmentRotationDuration = 10 * time.Second
	}
}

package unaligned

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestStore_Write(t *testing.T) {

	tempDir := t.TempDir()
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	partitionId := window.SharedUnalignedPartition
	s, err := NewStore(&partitionId, WithStoreOptions(tempDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(100, time.UnixMilli(60000))

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	// close the WAL
	err = s.Close()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err := os.ReadDir(tempDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)
}

func WithStoreOptions(path string) WALOption {
	return func(s *WAL) {
		s.storeDataPath = path
		s.segmentSize = 1024
		s.syncDuration = time.Second
		s.maxBatchSize = 1024 * 50
		s.segmentRotationDuration = 10 * time.Second
	}
}

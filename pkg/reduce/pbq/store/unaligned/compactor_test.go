package unaligned

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestCompactor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()

	pid := window.SharedUnalignedPartition
	// write some data files
	s, err := NewStore(&pid, WithStoreOptions(dataDir))
	assert.NoError(t, err)

	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(300, time.UnixMilli(60000))

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	eventDir := t.TempDir()
	/// write some delete events
	tracker, err := NewGCEventsTracker(ctx, WithEventsPath(eventDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	ts := time.UnixMilli(60000)
	windows := buildTestWindows(ts, 10, time.Second*10)
	for _, timedWindow := range windows {
		err = tracker.TrackGCEvent(timedWindow)
		assert.NoError(t, err)
	}

	err = s.Close()
	assert.NoError(t, err)
	err = tracker.Close()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err := os.ReadDir(eventDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)

	for _, file := range files {
		println(file.Name())
	}

	// create compactor with the data and event directories
	c, err := NewCompactor(&pid, dataDir, eventDir, WithCompactionDuration(time.Second*5), WithCompactorMaxFileSize(1024*1024*5))
	assert.NoError(t, err)

	err = c.Start(ctx)
	assert.NoError(t, err)

	err = c.Stop()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err = os.ReadDir(dataDir)
	assert.NoError(t, err)
	//assert.Equal(t, 1, len(files))

	// read from file and check if the data is correct
	d := newDecoder()

	// read the file
	file, err := os.OpenFile(filepath.Join(dataDir, files[0].Name()), os.O_RDONLY, 0644)
	assert.NoError(t, err)

	header, err := d.decodeHeader(file)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), header.Start.UnixMilli())
	assert.Equal(t, int64(math.MaxInt64), header.End.UnixMilli())
	assert.Equal(t, "slot-0", header.Slot)

	for {
		msg, _, err := d.decodeMessage(file)
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
				assert.NoError(t, err)
			}
		}
		if msg.EventTime.Before(windows[len(windows)-1].EndTime()) {
			assert.Fail(t, "not compacted")
		}
	}
}

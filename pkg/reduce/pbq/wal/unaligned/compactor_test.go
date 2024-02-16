package unaligned

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestCompactor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()

	pid := window.SharedUnalignedPartition
	// write some data files
	s, err := NewUnalignedWriteOnlyWAL(&pid, WithStoreOptions(dataDir))
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

func TestReplay_AfterCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()

	pid := window.SharedUnalignedPartition
	// write some data files
	s, err := NewUnalignedWriteOnlyWAL(&pid, WithStoreOptions(dataDir))
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

	// create compactor with the data and event directories
	c, err := NewCompactor(&pid, dataDir, eventDir, WithCompactionDuration(time.Second*5), WithCompactorMaxFileSize(1024*1024*5))
	assert.NoError(t, err)

	err = c.Start(ctx)
	assert.NoError(t, err)

	err = c.Stop()
	assert.NoError(t, err)

	wl, err := NewUnalignedReadWriteWAL(WithStoreOptions(dataDir))
	assert.NoError(t, err)

	// replay the messages
	readCh, errCh := wl.Replay()
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
	// first 101 messages will be compacted
	assert.Len(t, replayedMessages, 199)

	// order is important
	for i := 0; i < 199; i++ {
		assert.Equal(t, readMessages[i+101].EventTime.UnixMilli(), replayedMessages[i].EventTime.UnixMilli())
	}
}

func TestFilesInDir(t *testing.T) {
	dir := t.TempDir()
	// create some files
	for i := 0; i < 10; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf("file-%d", i)))
		assert.NoError(t, err)
		err = file.Close()
		assert.NoError(t, err)
	}

	files, err := filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 10)

	// add current file
	file, err := os.Create(filepath.Join(dir, "current-segment"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	files, err = filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 10)

	// remove all files except current
	for i := 0; i < 10; i++ {
		err = os.Remove(filepath.Join(dir, fmt.Sprintf("file-%d", i)))
		assert.NoError(t, err)
	}

	files, err = filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 0)

	// add 2 more files at the end
	file, err = os.Create(filepath.Join(dir, "file-1"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	file, err = os.Create(filepath.Join(dir, "file-2"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	files, err = filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// add another current file
	file, err = os.Create(filepath.Join(dir, "current-compacted"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	files, err = filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// remove except current
	err = os.Remove(filepath.Join(dir, "file-1"))
	assert.NoError(t, err)

	err = os.Remove(filepath.Join(dir, "file-2"))
	assert.NoError(t, err)

	files, err = filesInDir(dir)
	assert.NoError(t, err)
	assert.Len(t, files, 0)
}

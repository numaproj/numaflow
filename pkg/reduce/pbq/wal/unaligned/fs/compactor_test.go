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
	"errors"
	"fmt"
	"io"
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

	segmentDir := t.TempDir()
	defer func() {
		cleanupDir(segmentDir)
	}()

	compactDir := t.TempDir()
	defer func() {
		cleanupDir(compactDir)
	}()

	pid := window.SharedUnalignedPartition
	// write some data filesToReplay
	s, err := NewUnalignedWriteOnlyWAL(&pid, WithStoreOptions(segmentDir, compactDir))
	assert.NoError(t, err)

	keys := []string{"key-1", "key-2"}
	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(300, time.UnixMilli(60000), keys)

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	eventDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(eventDir)
	}()
	/// write some delete events
	tracker, err := NewGCEventsWAL(ctx, WithEventsPath(eventDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	ts := time.UnixMilli(60000)
	windows := buildTestWindows(ts, 10, time.Second*10, keys)
	for _, timedWindow := range windows {
		err = tracker.PersistGCEvent(timedWindow)
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
	c, err := NewCompactor(ctx, &pid, eventDir, segmentDir, compactDir, WithCompactionDuration(time.Second*5), WithCompactorMaxFileSize(1024*1024*5))
	assert.NoError(t, err)

	err = c.Start(ctx)
	assert.NoError(t, err)

	err = c.Stop()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err = os.ReadDir(segmentDir)
	assert.NoError(t, err)

	files, err = os.ReadDir(compactDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)

	// read from file and check if the data is correct
	d := newDecoder()

	// read the file
	file, err := os.OpenFile(filepath.Join(compactDir, files[0].Name()), os.O_RDONLY, 0644)
	assert.NoError(t, err)

	header, err := d.decodeHeader(file)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), header.Start.UnixMilli())
	assert.Equal(t, int64(math.MaxInt64), header.End.UnixMilli())
	assert.Equal(t, "slot-0", header.Slot)

	for {
		msg, _, err := d.decodeMessage(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
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

	segmentDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(segmentDir)
	}()

	compactDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(compactDir)
	}()

	pid := window.SharedUnalignedPartition
	// write some data files
	s, err := NewUnalignedWriteOnlyWAL(&pid, WithStoreOptions(segmentDir, compactDir))
	assert.NoError(t, err)

	keys := []string{"key-1", "key-2"}
	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(300, time.UnixMilli(60000), keys)

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	eventDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(eventDir)
	}()
	/// write some delete events
	tracker, err := NewGCEventsWAL(ctx, WithEventsPath(eventDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	ts := time.UnixMilli(60000)
	windows := buildTestWindows(ts, 10, time.Second*10, keys)
	for _, timedWindow := range windows {
		err = tracker.PersistGCEvent(timedWindow)
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
	c, err := NewCompactor(ctx, &pid, eventDir, segmentDir, compactDir, WithCompactionDuration(time.Second*5), WithCompactorMaxFileSize(1024*1024*5))
	assert.NoError(t, err)

	err = c.Start(ctx)
	assert.NoError(t, err)

	err = c.Stop()
	assert.NoError(t, err)

	sm := NewFSManager(segmentDir, compactDir, vertexInstance)
	wls, err := sm.DiscoverWALs(ctx)
	assert.NoError(t, err)
	assert.Len(t, wls, 1)

	wl := wls[0]

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
	err = wl.Close()
	assert.NoError(t, err)
}

func TestFilesInDir(t *testing.T) {
	dir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(dir)
	}()

	// create some files
	for i := 0; i < 10; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf("file-%d", i)))
		assert.NoError(t, err)
		err = file.Close()
		assert.NoError(t, err)
	}

	files, err := listFilesInDir(dir, currentWALPrefix, sortFunc)
	assert.NoError(t, err)
	assert.Len(t, files, 10)

	// add current file
	file, err := os.Create(filepath.Join(dir, "current-segment"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	files, err = listFilesInDir(dir, currentWALPrefix, sortFunc)
	assert.NoError(t, err)
	assert.Len(t, files, 10)

	// remove all files except current
	for i := 0; i < 10; i++ {
		err = os.Remove(filepath.Join(dir, fmt.Sprintf("file-%d", i)))
		assert.NoError(t, err)
	}

	files, err = listFilesInDir(dir, currentWALPrefix, sortFunc)
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

	files, err = listFilesInDir(dir, currentWALPrefix, sortFunc)
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// add another current file
	file, err = os.Create(filepath.Join(dir, "current-compacted"))
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)

	files, err = listFilesInDir(dir, currentWALPrefix, sortFunc)
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// remove except current
	err = os.Remove(filepath.Join(dir, "file-1"))
	assert.NoError(t, err)

	err = os.Remove(filepath.Join(dir, "file-2"))
	assert.NoError(t, err)

	files, err = listFilesInDir(dir, currentWALPrefix, sortFunc)
	assert.NoError(t, err)
	assert.Len(t, files, 0)
}

func TestCompactor_ContextClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	segmentDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(segmentDir)
	}()

	compactDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(compactDir)
	}()

	pid := window.SharedUnalignedPartition
	// write some data files
	s, err := NewUnalignedWriteOnlyWAL(&pid, WithStoreOptions(segmentDir, compactDir))
	assert.NoError(t, err)

	keys := []string{"key-1", "key-2"}
	// create read messages
	readMessages := testutils.BuildTestReadMessagesIntOffset(300, time.UnixMilli(60000), keys)

	// write the messages
	for _, readMessage := range readMessages {
		err = s.Write(&readMessage)
		assert.NoError(t, err)
	}

	eventDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(eventDir)
	}()
	/// write some delete events
	tracker, err := NewGCEventsWAL(ctx, WithEventsPath(eventDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	ts := time.UnixMilli(60000)
	windows := buildTestWindows(ts, 10, time.Second*10, keys)
	for _, timedWindow := range windows {
		err = tracker.PersistGCEvent(timedWindow)
		assert.NoError(t, err)
	}

	err = s.Close()
	assert.NoError(t, err)
	err = tracker.Close()
	assert.NoError(t, err)

	// create compactor with the data and event directories
	c, err := NewCompactor(ctx, &pid, eventDir, segmentDir, compactDir, WithCompactionDuration(time.Second*5), WithCompactorMaxFileSize(1024*1024*5))
	assert.NoError(t, err)

	err = c.Start(ctx)
	assert.NoError(t, err)

	cancel()
	files, _ := filesInDir(segmentDir, currentWALPrefix)
	for _, file := range files {
		println(file.Name())
	}
	time.Sleep(3 * time.Second)
	err = c.Stop()
	if err != nil {
		println(err.Error())
	}
	assert.NoError(t, err)
}

func Test_buildCompactionKeyMap(t *testing.T) {
	ctx := context.Background()

	eventDir := t.TempDir()
	// delete all the files in the directory at the end
	defer func() {
		cleanupDir(eventDir)
	}()
	/// write some delete events
	ewl, err := NewGCEventsWAL(ctx, WithEventsPath(eventDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	testWindows := []window.TimedWindow{
		window.NewUnalignedTimedWindow(time.UnixMilli(60000), time.UnixMilli(60010), "slot-0", []string{"key-1", "key-2"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60010), time.UnixMilli(60020), "slot-0", []string{"key-3", "key-4"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60020), time.UnixMilli(60030), "slot-0", []string{"key-5", "key-6"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60030), time.UnixMilli(60040), "slot-0", []string{"", ""}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60040), time.UnixMilli(60050), "slot-0", []string{"key-7", "key-8"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60050), time.UnixMilli(60060), "slot-0", []string{"key-5", "key-7"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60060), time.UnixMilli(60070), "slot-0", []string{"key-1", "key-2"}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60070), time.UnixMilli(60080), "slot-0", []string{""}),
		window.NewUnalignedTimedWindow(time.UnixMilli(60090), time.UnixMilli(60100), "slot-0", []string{"", "", ""}),
	}

	for _, timedWindow := range testWindows {
		err = ewl.PersistGCEvent(timedWindow)
		assert.NoError(t, err)
	}

	err = ewl.Close()
	assert.NoError(t, err)

	c := &compactor{
		compactKeyMap:   make(map[string]int64),
		gcEventsWALPath: eventDir,
		dc:              newDecoder(),
	}

	eFiles, err := filesInDir(eventDir, currentWALPrefix)
	assert.NoError(t, err)

	err = c.buildCompactionKeyMap(eFiles)

	assert.Len(t, c.compactKeyMap, 8)
	assert.Equal(t, int64(60030), c.compactKeyMap["key-5:key-6"])
	assert.Equal(t, int64(60050), c.compactKeyMap["key-7:key-8"])
	assert.Equal(t, int64(60060), c.compactKeyMap["key-5:key-7"])
	assert.Equal(t, int64(60070), c.compactKeyMap["key-1:key-2"])
	assert.Equal(t, int64(60040), c.compactKeyMap[":"])
	assert.Equal(t, int64(60100), c.compactKeyMap["::"])
}

// cleanup_dir removes all the files in the directory
func cleanupDir(dir string) {
	_ = os.RemoveAll(dir)
}

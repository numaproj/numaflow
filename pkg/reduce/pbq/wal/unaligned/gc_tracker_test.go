package unaligned

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/window"
)

func TestGcEventsTracker_TrackGCEvent(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	tracker, err := NewGCEventsTracker(ctx, WithEventsPath(tempDir), WithGCTrackerSyncDuration(100*time.Millisecond), WithGCTrackerRotationDuration(time.Second))
	assert.NoError(t, err)

	// build test windows
	ts := time.UnixMilli(60000)
	windows := buildTestWindows(ts, 100, time.Second, []string{"key-1", "key-2"})
	for _, timedWindow := range windows {
		err = tracker.TrackGCEvent(timedWindow)
		time.Sleep(time.Millisecond * 10)
		assert.NoError(t, err)
	}

	err = tracker.Close()
	assert.NoError(t, err)

	// list all the files in the directory
	files, err := os.ReadDir(tempDir)
	assert.NoError(t, err)
	assert.NotEmpty(t, files)
}

func buildTestWindows(ts time.Time, count int, windowSize time.Duration, keys []string) []window.TimedWindow {
	var windows = make([]window.TimedWindow, 0, count)
	for i := 0; i < count; i++ {
		windows = append(windows, window.NewUnalignedTimedWindow(ts, ts.Add(windowSize), "slot-0", keys))
		ts = ts.Add(windowSize)
	}

	return windows
}

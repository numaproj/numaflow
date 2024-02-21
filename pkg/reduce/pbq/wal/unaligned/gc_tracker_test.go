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

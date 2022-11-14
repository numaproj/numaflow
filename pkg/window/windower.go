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

package window

import (
	"time"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
)

// AlignedWindow interface represents a bounded window at a moment in time
// for example in case of fixed and sliding windows, AlignedWindow will have the
// same start and end time as the initial window that element is slotted in to.
// However, same cannot be said about a session window. Window Boundaries will keep
// changing up until a session is closed or times out.
type AlignedWindow interface {
	// StartTime returns the start time of the window
	StartTime() time.Time
	// EndTime returns the end time of the window
	EndTime() time.Time
	// AddKey adds a key to the window
	AddKey(string)
	// Partitions returns an array of partition ids
	Partitions() []partition.ID
	// Keys returns an array of keys
	Keys() []string
}

// Windower manages windows
// Will be implemented by each of the windowing strategies.
type Windower interface {
	// AssignWindow assigns the event to the window based on give window configuration.
	AssignWindow(eventTime time.Time) []AlignedWindow
	// CreateWindow creates a window for a supplied interval
	CreateWindow(aw AlignedWindow) AlignedWindow
	// GetWindow returns a keyed window for a supplied interval
	GetWindow(aw AlignedWindow) AlignedWindow
	// RemoveWindows returns list of window(s) that can be closed
	RemoveWindows(time time.Time) []AlignedWindow
}

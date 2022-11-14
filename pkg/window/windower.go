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

// AlignedKeyedWindower represents a bounded window (i.e., it will have a definite start and end time), and the
// keys that also fall into the same window.
type AlignedKeyedWindower interface {
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

// Windower manages AlignedKeyedWindower
// Will be implemented by each of the windowing strategies.
type Windower interface {
	// AssignWindow assigns the event to the window based on give window configuration.
	AssignWindow(eventTime time.Time) []AlignedKeyedWindower
	// CreateWindow creates a window for a supplied interval
	CreateWindow(aw AlignedKeyedWindower) AlignedKeyedWindower
	// GetWindow returns a keyed window for a supplied interval
	GetWindow(aw AlignedKeyedWindower) AlignedKeyedWindower
	// RemoveWindows returns list of window(s) that can be closed
	RemoveWindows(time time.Time) []AlignedKeyedWindower
}

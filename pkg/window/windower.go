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

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// AlignedWindower defines a window that is aligned and bounded by start and end time.
type AlignedWindower interface {
	// StartTime returns the start time of the window
	StartTime() time.Time
	// EndTime returns the end time of the window
	EndTime() time.Time
}

// AlignedKeyedWindower represents a keyed or non-keyed aligned window (bounded by start and end).
type AlignedKeyedWindower interface {
	AlignedWindower
	// AddSlot adds a slot to the window. Slots are hash-ranges for keys.
	AddSlot(string)
	// Partitions returns an array of partition ids
	// partitionId is a combination of start and end time of the window and the slot
	Partitions() []partition.ID
	// Slots returns an array of slots
	Slots() []string
}

// Windower manages AlignedKeyedWindower
// Will be implemented by each of the windowing strategies.
type Windower interface {
	// AssignWindow assigns the event to the window based on give window configuration.
	AssignWindow(eventTime time.Time) []AlignedKeyedWindower
	// InsertIfNotPresent inserts window to the list of active windows if not present
	// if present it will return the window
	InsertIfNotPresent(aw AlignedKeyedWindower) (AlignedKeyedWindower, bool)
	// RemoveWindows returns list of window(s) that can be closed
	RemoveWindows(time time.Time) []AlignedKeyedWindower
	// NextWindowToBeClosed returns the next window yet to be closed.
	// will be used by the data forwarder to check if the late message can be considered for processing.
	NextWindowToBeClosed() AlignedKeyedWindower
}

type TimedWindower interface {
	// AssignWindows assigns the event to the window based on give window configuration.
	AssignWindows(message *isb.ReadMessage) []*TimedWindowRequest
	// CloseWindows closes the windows that are past the watermark
	CloseWindows(time time.Time) []*TimedWindowRequest
	// NextWindowToBeClosed returns the next window yet to be closed.
	NextWindowToBeClosed() TimedWindow
}

type TimedWindow interface {
	// StartTime returns the start time of the window
	StartTime() time.Time
	// EndTime returns the end time of the window
	EndTime() time.Time
	// Slot returns the slot to which the window belongs
	Slot() string
	// Partition returns the unique partition id of the window
	// could be combination of startTime, endTime and slot
	Partition() *partition.ID
	// Merge merges the window with the new window
	Merge(tw TimedWindow)
	// Expand expands the window end time to the new endTime
	Expand(endTime time.Time)
}

// TimedWindowRequest represents the operation on the window
type TimedWindowRequest struct {
	// event type of the operation on the windows
	Event Event
	// IsbMessage represents the isb message
	IsbMessage *isb.ReadMessage
	// ID represents the partition id
	// this is to map to the pbq instance to which the message should be assigned
	ID *partition.ID
	// windows is the list of windows on which the operation is performed
	Windows []TimedWindow
}

// Event represents the event type of the operation on the window
type Event int

const (
	Open Event = iota
	Delete
	Close
	Merge
	Append
	Expand
)

func (e Event) String() string {
	switch e {
	case Open:
		return "Open"
	case Delete:
		return "Delete"
	case Close:
		return "Close"
	case Merge:
		return "Merge"
	case Append:
		return "Append"
	default:
		return "Unknown"
	}
}

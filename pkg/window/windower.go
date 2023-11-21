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

type TimedWindower interface {
	// Strategy returns the window strategy
	Strategy() Strategy
	// AssignWindows assigns the event to the window based on give window configuration.
	AssignWindows(message *isb.ReadMessage) []*TimedWindowRequest
	// CloseWindows closes the windows that are past the watermark
	CloseWindows(time time.Time) []*TimedWindowRequest
	// InsertWindow inserts window to the list of active windows
	InsertWindow(tw TimedWindow)
	// NextWindowToBeClosed returns the next window yet to be closed.
	NextWindowToBeClosed() TimedWindow
	// DeleteClosedWindows deletes the windows from the closed windows list
	DeleteClosedWindows(response *TimedWindowResponse)
	// OldestWindowEndTime returns the end time of the oldest closed window
	OldestWindowEndTime() time.Time
}

type TimedWindow interface {
	// StartTime returns the start time of the window
	StartTime() time.Time
	// EndTime returns the end time of the window
	EndTime() time.Time
	// Slot returns the slot to which the window belongs
	Slot() string
	// Keys returns the keys of the window
	Keys() []string
	// Partition returns the unique partition id of the window
	// could be combination of startTime, endTime and slot
	Partition() *partition.ID
	// Merge merges the window with the new window
	Merge(tw TimedWindow)
	// Expand expands the window end time to the new endTime
	Expand(endTime time.Time)
}

// timedWindow implements TimedWindow
type timedWindow struct {
	startTime time.Time
	endTime   time.Time
	slot      string
	keys      []string
}

// NewWindowFromPartition returns a new TimedWindow for the given partition id.
func NewWindowFromPartition(id *partition.ID) TimedWindow {
	return &timedWindow{
		startTime: id.Start,
		endTime:   id.End,
		slot:      id.Slot,
	}
}

// NewWindowFromPartitionAndKeys returns a new TimedWindow for the given partition id and keys.
func NewWindowFromPartitionAndKeys(id *partition.ID, keys []string) TimedWindow {
	return &timedWindow{
		startTime: id.Start,
		endTime:   id.End,
		slot:      id.Slot,
		keys:      keys,
	}
}

func (w *timedWindow) StartTime() time.Time {
	return w.startTime
}

func (w *timedWindow) EndTime() time.Time {
	return w.endTime
}

func (w *timedWindow) Slot() string {
	return w.slot
}

func (w *timedWindow) Keys() []string {
	return w.keys
}

func (w *timedWindow) Partition() *partition.ID {
	return &partition.ID{
		Start: w.startTime,
		End:   w.endTime,
		Slot:  w.slot,
	}
}

func (w *timedWindow) Merge(tw TimedWindow) {
	// expand the start and end to accommodate the new window
	if tw.StartTime().Before(w.startTime) {
		w.startTime = tw.StartTime()
	}

	if tw.EndTime().After(w.endTime) {
		w.endTime = tw.EndTime()
	}
}

func (w *timedWindow) Expand(endTime time.Time) {
	if endTime.After(w.endTime) {
		w.endTime = endTime
	}
}

// TimedWindowRequest represents the operation on the window
type TimedWindowRequest struct {
	// Operation of the operation on the windows
	Operation Operation
	// ReadMessage represents the isb message
	ReadMessage *isb.ReadMessage
	// ID represents the partition id
	// this is to map to the pbq instance to which the message should be assigned
	ID *partition.ID
	// windows is the list of windows on which the operation is performed
	Windows []TimedWindow
}

type TimedWindowResponse struct {
	// ReadMessage represents the isb message
	WriteMessages []*isb.WriteMessage
	// ID represents the partition id to which the response belongs
	Window TimedWindow
}

// Operation represents the event type of the operation on the window
type Operation int

const (
	Open Operation = iota
	Delete
	Close
	Merge
	Append
	Expand
)

func (e Operation) String() string {
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
	case Expand:
		return "Expand"
	default:
		return "Unknown"
	}
}

// Strategy represents the windowing strategy
type Strategy int

const (
	Fixed Strategy = iota
	Sliding
	Session
	Global
)

func (s Strategy) String() string {
	switch s {
	case Fixed:
		return "Fixed"
	case Sliding:
		return "Sliding"
	case Session:
		return "Session"
	case Global:
		return "Global"
	default:
		return "Unknown"
	}
}

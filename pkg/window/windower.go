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

// TimedWindower is the interface for windowing strategy.
// It manages the lifecycle of timed windows for a reduce vertex.
// It maintains a list of timed windows locally, generates window requests to be sent to the reduce UDF,
// and reflects the changes to the list of timed windows based on the response from the UDF.
type TimedWindower interface {
	// Strategy returns the window strategy.
	Strategy() Strategy
	// Type returns the window type.
	Type() Type
	// AssignWindows assigns the event to the window based on give window configuration.
	AssignWindows(message *isb.ReadMessage) []*TimedWindowRequest
	// CloseWindows closes the windows that are past the watermark.
	CloseWindows(time time.Time) []*TimedWindowRequest
	// InsertWindow inserts a window to the list of active windows.
	InsertWindow(tw TimedWindow)
	// NextWindowToBeClosed returns the next window yet to be closed.
	NextWindowToBeClosed() TimedWindow
	// DeleteClosedWindow deletes the window from the closed windows list.
	DeleteClosedWindow(response *TimedWindowResponse)
	// OldestWindowEndTime returns the end time of the oldest window among both active and closed windows.
	// If there are no windows, it returns -1.
	OldestWindowEndTime() time.Time
}

// TimedWindow represents a time-based window.
type TimedWindow interface {
	// StartTime returns the start time of the window.
	StartTime() time.Time
	// EndTime returns the end time of the window.
	EndTime() time.Time
	// Slot returns the slot to which the window belongs.
	Slot() string
	// Partition returns the partition id of the window, partition is
	// combination of start time, end time and slot.
	// for aligned windows, this will be used to map to the pbq instance
	// for unaligned windows, we use a single pbq instance for all the windows
	Partition() *partition.ID
	// Keys returns the keys of the window tracked for Unaligned windows.
	// This will return empty for Aligned windows.
	Keys() []string
	// Merge merges the window with the new window. It is used only for
	// Unaligned window.
	Merge(tw TimedWindow)
	// Expand expands the window end time to the new endTime. It is used only for
	// Unaligned window.
	Expand(endTime time.Time)
}

// timedWindow implements TimedWindow.
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

// TimedWindowRequest represents the operation to be performed on the window. TimedWindowRequest is sent
// to the UDF and it contains enough context to execute the operation.
type TimedWindowRequest struct {
	// Operation is the `Operation` on the windows
	Operation Operation
	// ReadMessage represents the isb message
	ReadMessage *isb.ReadMessage
	// ID represents the partition id
	// this is to map to the pbq instance to which the message should be assigned
	ID *partition.ID
	// windows is the list of windows on which the operation is performed
	Windows []TimedWindow
}

// TimedWindowResponse is the response from the UDF based on how the result is propagated back.
// It could be one or more responses based on how many results the user is streaming out.
type TimedWindowResponse struct {
	// WriteMessage represents the isb message
	WriteMessage *isb.WriteMessage
	// Window represents the window to which the message belongs
	Window TimedWindow
	// EOF represents the end of the response for the given window.
	// When EOF is true, it will be just a metadata payload, there won't be any WriteMessage.
	EOF bool
}

// Operation represents the event type of the operation on the window
type Operation int

const (
	// Open is create a new Window (Open the Book).
	Open Operation = iota
	// Delete closes the partition (this means all the keyed-windows have been closed).
	// PBQ gets closed when Delete is called.
	Delete
	// Close operation for the keyed-window (Close of Book). Only the keyed-window on the SDK side will be closed,
	// other keyed-windows for the same partition can be open. `Delete` has to be called once all the keyed-windows
	// are closed.
	Close
	// Merge merges two or more windows, particularly used for SessionWindows.
	// Perhaps in future we will use it for hot-key partitioning.
	Merge
	// Append inserts more data into the opened Window. Append implicitly does Open if window has not been opened yet.
	Append
	// Expand expands the existing window, used in SessionWindow after adding a new element or after a window merge operation.
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

// Type represents window type
type Type int

const (
	Aligned Type = iota
	Unaligned
)

func (t Type) String() string {
	switch t {
	case Aligned:
		return "Aligned"
	case Unaligned:
		return "Unaligned"
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

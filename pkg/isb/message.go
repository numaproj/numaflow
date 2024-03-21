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

package isb

import (
	"time"
)

// MessageKind represents the message type of the payload.
type MessageKind int16

const (
	Data MessageKind = iota // Data payload
	WMB                     // Watermark Barrier
)

func (mt MessageKind) String() string {
	switch mt {
	case Data:
		return "Data"
	case WMB:
		return "WMB"
	default:
		return "Unknown"
	}
}

// MessageInfo is the message information window of the payload.
// The contents inside the MessageInfo can be interpreted differently based on the MessageKind.
type MessageInfo struct {
	// EventTime when
	// MessageKind == Data represents the event time of the message
	// MessageKind == WMB, value is ignored
	EventTime time.Time
	// IsLate when
	// MessageKind == Data, IsLate is used to indicate if the message is a late data (assignment happens at source)
	// MessageKind == WMB, value is ignored
	IsLate bool
}

// MessageMetadata is the metadata of the message
type MessageMetadata struct {
	// NumDelivered is the number of times the message has been delivered.
	NumDelivered uint64
}

// Header is the header of the message
type Header struct {
	MessageInfo
	// Kind indicates the kind of Message
	Kind MessageKind
	// ID is used for exactly-once-semantics. ID is usually populated from the offset, if offset is available.
	ID string
	// Keys is (key,value) in the map-reduce paradigm will be used for reduce operation, last key in the list
	// will be used for conditional forwarding
	Keys []string
	// Headers is the headers of the message which can be used to store and propagate source headers like kafka headers,
	// http headers and Numaflow platform headers like tracing headers etc.
	Headers map[string]string
}

// Body is the body of the message
type Body struct {
	Payload []byte
}

// Message is inter step message
type Message struct {
	Header
	Body
}

// ReadMessage is the message read from the buffer.
type ReadMessage struct {
	Message
	ReadOffset Offset
	Watermark  time.Time
	// Metadata is the metadata of the message after a message is read from the buffer.
	Metadata MessageMetadata
}

// ToReadMessage converts Message to a ReadMessage by providing the offset and watermark
func (m *Message) ToReadMessage(ot Offset, wm time.Time) *ReadMessage {
	return &ReadMessage{Message: *m, ReadOffset: ot, Watermark: wm}
}

// WriteMessage is a wrapper for an isb message with tag information which will be used
// for conditional forwarding.
type WriteMessage struct {
	Message
	Tags []string
}

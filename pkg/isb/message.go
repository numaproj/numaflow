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

// MessageType represents the message type of the payload.
type MessageType int16

const (
	Data MessageType = 1 << iota // Data payload
	WMB                          // Watermark Barrier
)

func (mt MessageType) String() string {
	switch mt {
	case mt & Data:
		return "Data"
	case mt & WMB:
		return "WMB"
	default:
		return "Unknown"
	}
}

// MessageInfo is the message information window of the payload.
// The contents inside the MessageInfo can be interpreted differently based on the MessageType.
type MessageInfo struct {
	// EventTime when
	// MessageType == Data represents the event time of the message
	// MessageType == WMB, value is ignored
	EventTime time.Time
	// IsLate when
	// MessageType == Data, IsLate is used to indicate if the message is a late data (assignment happens at source)
	// MessageType == WMB, value is ignored
	IsLate bool
}

// Header is the header of the message
type Header struct {
	MessageInfo
	// Kind indicates the kind of Message
	Kind MessageType
	// ID is used for exactly-once-semantics. ID is usually populated from the offset, if offset is available.
	ID string
	// Key is (key,value) in the map-reduce paradigm which will be used for conditional forwarding.
	Key string
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
}

// ToReadMessage converts Message to a ReadMessage by providing the offset and watermark
func (m *Message) ToReadMessage(ot Offset, wm time.Time) *ReadMessage {
	return &ReadMessage{Message: *m, ReadOffset: ot, Watermark: wm}
}

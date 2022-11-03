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
	"encoding/json"
	"time"
)

// PaneInfo is the time window of the payload.
type PaneInfo struct {
	EventTime time.Time
	StartTime time.Time
	EndTime   time.Time
	// IsLate is used to indicate if it's a late data .
	IsLate bool
}

// Header is the header of the message
type Header struct {
	PaneInfo
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

// MarshalBinary encodes header to a binary format
func (h Header) MarshalBinary() (data []byte, err error) {
	return json.Marshal(h)
}

// MarshalBinary encodes header to a binary format
func (b Body) MarshalBinary() (data []byte, err error) {
	return json.Marshal(b)
}

// UnmarshalBinary decodes header from the binary format
func (h *Header) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &h)
}

// UnmarshalBinary decodes header from the binary format
func (b *Body) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &b)
}

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

package messages

import (
	"encoding"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"time"
)

// Message is the representation of a message flowing through the numaflow
type Message interface {
	ReadMessage
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	// SetID sets the ID for a message
	SetID(id string)
	// ID is the unique identifier for a message
	ID() string
	// SetEventTime represents the timestamp of the message depending on the time characteristics of the pipeline.
	// please note that EventTime does not mean only the event time.
	// This is intended to be set once and should be immutable. Therefore, an error could be thrown if
	// an attempt is made to overwrite the time on a message.
	SetEventTime(t time.Time)
	// EventTime returns the timestamp of the message. Calling Time() on a message should give a meaningful error
	// for the user to understand that a time has not been set yet.
	EventTime() time.Time
	// Key returns the key of the message
	Key() string
	// Payload returns the payload received from the source it was read.
	Payload() []byte
}

// ReadMessage exposes functions to set and get in-vertex stream characteristics of a NumaMessage.
type ReadMessage interface {
	// Offset returns the offset of the message from the source it was read.
	Offset() isb.Offset
	// SetWatermark sets the watermark on a message read from a source.
	SetWatermark(watermark processor.Watermark)
	// Watermark returns the watermark for a message.
	Watermark() processor.Watermark
	// Ack acknowledges a message
	Ack()
}

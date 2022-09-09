package messages

import (
	"encoding"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"time"
)

// NumaMessage is the representation of a message flowing through the numaflow
type NumaMessage interface {
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
	SetEventTime(t time.Time) error
	// EventTime returns the timestamp of the message. Calling Time() on a message should give a meaningful error
	// for the user to understand that a time has not been set yet.
	EventTime() (time.Time, error)
	// Key returns the key of the message
	Key() string
	// Payload returns the payload received from the source it was read.
	Payload() ([]byte, error)
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

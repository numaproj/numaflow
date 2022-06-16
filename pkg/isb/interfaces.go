/*
Package isb defines and implements the inter-step buffer and the communication. The inter-step communication is reading from the
previous step (n-1th vertex in the DAG), processing it, conditionally forwarding to zero, one or all the neighboring steps (nth vertices)
and then acknowledging back to the previous step that we are done with processing.
*/

package isb

import (
	"context"
	"io"
	"strconv"
)

const (
	PendingNotAvailable = int64(-1)
	RateNotAvailable    = float64(-1)
)

// Ratable is the interface that wraps the Rate method.
type Ratable interface {
	// Rate returns the rough rate (messages/second), this is used for auto-scaling calculation
	Rate(context.Context) (float64, error)
}

// BufferWriter is the buffer to which we are writing.
type BufferWriter interface {
	BufferWriterInformation
	io.Closer
	Write(context.Context, []Message) ([]Offset, []error)
}

// BufferReader is the buffer from which we are reading.
type BufferReader interface {
	BufferReaderInformation
	io.Closer
	// Read reads a chunk of messages and returns at the first occurrence of an error. Error does not indicate that the
	// array of result is empty, the callee should process all the elements in the array even if the error is set. Read
	// will not mark the message in the buffer as "READ" if the read for that index is erring.
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages.
	Read(context.Context, int64) ([]*ReadMessage, error)
	// Ack acknowledges an array of offset.
	Ack(context.Context, []Offset) []error
	// Pending returns the pending messages number.
	Pending(context.Context) (int64, error)
}

// BufferReaderInformation has information regarding the buffer we are reading from.
type BufferReaderInformation interface {
	GetName() string
}

// BufferWriterInformation has information regarding the buffer we are writing to.
type BufferWriterInformation interface {
	GetName() string
}

// Offset is an interface used in the ReadMessage referencing offset information.
type Offset interface {
	// String return the offset identifier
	String() string
	// Sequence returns a sequence id which can be used to index into the buffer (ISB)
	Sequence() (int64, error)
	// AckIt is used to ack the offset
	// This is often used when the BufferReader can not simply use the offset identifier to ack the message,
	// then the work can be done in this function, and call it in BufferReader Ack() function implementation.
	AckIt() error
}

// SimpleOffset is an Offset convenient function for implementations without needing AckIt()
type SimpleOffset func() string

func (so SimpleOffset) String() string {
	return so()
}

func (so SimpleOffset) Sequence() (int64, error) {
	return strconv.ParseInt(so(), 10, 64)
}

func (so SimpleOffset) AckIt() error {
	return nil
}

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

/*
Package isb defines and implements the inter-step buffer and the communication. The inter-step communication is reading from the
previous step (n-1th vertex in the DAG), processing it, conditionally forwarding to zero, one or all the neighboring steps (nth vertices)
and then acknowledging back to the previous step that we are done with processing.
*/

package isb

import (
	"context"
	"io"
	"math"
)

const PendingNotAvailable = int64(math.MinInt64)

// BufferWriter is the buffer to which we are writing.
type BufferWriter interface {
	BufferWriterInformation
	io.Closer
	Write(context.Context, []Message) ([]Offset, []error)
	WriteNew(context.Context, Message) (Offset, error)
}

// BufferReader is the buffer from which we are reading.
type BufferReader interface {
	BufferReaderInformation
	io.Closer
	// Read reads a chunk of messages and returns at the first occurrence of an error. Error does not indicate that the
	// array of result is empty, the callee should process all the elements in the array even if the error is set. Read
	// will not mark the message in the buffer as "READ" if the read for that index is erring.
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during the restart we will have to reprocess all unacknowledged messages.
	Read(context.Context, int64) ([]*ReadMessage, error)
	// Ack acknowledges an array of offset.
	Ack(context.Context, []Offset) []error
	// NoAck cancels acknowledgement of an array of offset.
	NoAck(context.Context, []Offset)
	// Pending returns the count of pending messages.
	Pending(context.Context) (int64, error)
}

// LagReader is the interface that wraps the Pending method and GetName method.
// will be used by the metrics server to get the pending messages count.
type LagReader interface {
	GetName() string
	// Pending returns the pending messages number.
	Pending(context.Context) (int64, error)
}

// BufferReader can be used as LagReader.
var _ LagReader = (BufferReader)(nil)

// BufferReaderInformation has information regarding the buffer we are reading from.
type BufferReaderInformation interface {
	// GetName returns the name.
	GetName() string
	// GetPartitionIdx returns the partition ID.
	GetPartitionIdx() int32
}

// BufferWriterInformation has information regarding the buffer we are writing to.
type BufferWriterInformation interface {
	// GetName returns the name.
	GetName() string
	// GetPartitionIdx returns the partition ID.
	GetPartitionIdx() int32
}

// Offset is an interface used in the ReadMessage referencing offset information.
type Offset interface {
	// String returns the offset identifier
	String() string
	// Sequence returns a sequence id which can be used to index into the buffer (ISB)
	Sequence() (int64, error)
	// AckIt is used to ack the offset
	// This is often used when the BufferReader can not simply use the offset identifier to ack the message,
	// then the work can be done in this function, and call it in BufferReader Ack() function implementation.
	AckIt() error
	// NoAck to indicate the offset no longer needs to be acknowledged
	// It is used when error occurs, and we want to reprocess the batch to indicate acknowledgement no
	// longer needed.
	NoAck() error
	// PartitionIdx returns the partition index to which the offset belongs to.
	PartitionIdx() int32
}

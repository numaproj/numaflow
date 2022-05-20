package isb

import "fmt"

// MessageWriteErr is associated with message write errors.
type MessageWriteErr struct {
	Name    string
	Header  Header
	Body    Body
	Message string
}

func (e MessageWriteErr) Error() string {
	return fmt.Sprintf("(%s) %s Header: %#v Body:%#v", e.Name, e.Message, e.Header, e.Body)
}

// BufferWriteErr when we cannot write to the buffer because of a full buffer.
type BufferWriteErr struct {
	Name        string
	Full        bool
	InternalErr bool
	Message     string
}

func (e BufferWriteErr) Error() string {
	return fmt.Sprintf("(%s) %s %#v", e.Name, e.Message, e)
}

// IsFull returns true if buffer is full.
func (e BufferWriteErr) IsFull() bool {
	return e.Full
}

// IsInternalErr returns true if writing is failing due to a buffer internal error.
func (e BufferWriteErr) IsInternalErr() bool {
	return e.InternalErr
}

// MessageAckErr is for acknowledgement errors.
type MessageAckErr struct {
	Name    string
	Offset  Offset
	Message string
}

func (e MessageAckErr) Error() string {
	return fmt.Sprintf("(%s) %s", e.Name, e.Message)
}

// BufferReadErr when we cannot read from the buffer.
type BufferReadErr struct {
	Name        string
	Empty       bool
	InternalErr bool
	Message     string
}

func (e BufferReadErr) Error() string {
	return fmt.Sprintf("(%s) %s %#v", e.Name, e.Message, e)
}

// IsEmpty returns true if buffer is empty.
func (e BufferReadErr) IsEmpty() bool {
	return e.Empty
}

// IsInternalErr returns true if reading is failing due to a buffer internal error.
func (e BufferReadErr) IsInternalErr() bool {
	return e.InternalErr
}

// MessageReadErr is associated with message read errors.
type MessageReadErr struct {
	Name    string
	Header  []byte
	Body    []byte
	Message string
}

func (e MessageReadErr) Error() string {
	return fmt.Sprintf("(%s) %s Header: %s Body:%s", e.Name, e.Message, string(e.Header), string(e.Body))
}

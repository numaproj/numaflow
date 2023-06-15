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

// PartitionWriteErr when we cannot write to the partition because of a full partition.
type PartitionWriteErr struct {
	Name        string
	Full        bool
	InternalErr bool
	Message     string
}

func (e PartitionWriteErr) Error() string {
	return fmt.Sprintf("(%s) %s %#v", e.Name, e.Message, e)
}

// IsFull returns true if partition is full.
func (e PartitionWriteErr) IsFull() bool {
	return e.Full
}

// IsInternalErr returns true if writing is failing due to a partition internal error.
func (e PartitionWriteErr) IsInternalErr() bool {
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

// PartitionReadErr when we cannot read from the partition.
type PartitionReadErr struct {
	Name        string
	Empty       bool
	InternalErr bool
	Message     string
}

func (e PartitionReadErr) Error() string {
	return fmt.Sprintf("(%s) %s %#v", e.Name, e.Message, e)
}

// IsEmpty returns true if partition is empty.
func (e PartitionReadErr) IsEmpty() bool {
	return e.Empty
}

// IsInternalErr returns true if reading is failing due to a partition internal error.
func (e PartitionReadErr) IsInternalErr() bool {
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

// NonRetryablePartitionWriteErr indicates that the partition is full and the writer, based on user specification, decides to not retry.
type NonRetryablePartitionWriteErr struct {
	Name    string
	Message string
}

func (e NonRetryablePartitionWriteErr) Error() string {
	return fmt.Sprintf("(%s) %s %#v", e.Name, e.Message, e)
}

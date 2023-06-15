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

/* package simplepartition is in memory ring partition that implements the isb interface. This should be used only for local development
and testing purposes. Exactly-Once is not implemented because it is a side effect. The locking implementation is very coarse.
*/

package simplepartition

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

// InMemoryPartition implements ISB interface.
type InMemoryPartition struct {
	name         string
	size         int64
	elements     []elem
	writeIdx     int64
	readIdx      int64
	partitionIdx int32
	options      *options
	rwlock       *sync.RWMutex
}

var _ isb.PartitionReader = (*InMemoryPartition)(nil)
var _ isb.PartitionWriter = (*InMemoryPartition)(nil)

// elem is the element stored in the partition.
type elem struct {
	header  []byte
	body    []byte
	dirty   bool
	ack     bool
	pending bool
}

// NewInMemoryPartition returns a new partition.
func NewInMemoryPartition(name string, size int64, partition int32, opts ...Option) *InMemoryPartition {

	partitionOptions := &options{
		readTimeOut:                  time.Second,                // default read time out
		partitionFullWritingStrategy: v1alpha1.RetryUntilSuccess, // default elements full writing strategy
	}

	for _, o := range opts {
		_ = o(partitionOptions)
	}

	sb := &InMemoryPartition{
		name:         name,
		size:         size,
		elements:     make([]elem, size),
		writeIdx:     int64(0),
		readIdx:      int64(0),
		partitionIdx: partition,
		rwlock:       new(sync.RWMutex),
		options:      partitionOptions,
	}
	return sb
}

// Stringer
func (b *InMemoryPartition) String() string {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return fmt.Sprintf("(%s) size:%d readIdx:%d writeIdx:%d", b.name, b.size, b.readIdx, b.writeIdx)
}

// GetName returns the partition name.
func (b *InMemoryPartition) GetName() string {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.name
}

// GetPartitionIdx returns the partitionIdx.
func (b *InMemoryPartition) GetPartitionIdx() int32 {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.partitionIdx
}

func (b *InMemoryPartition) Pending(_ context.Context) (int64, error) {
	// TODO: not implemented
	return isb.PendingNotAvailable, nil
}

// Close does nothing.
func (b *InMemoryPartition) Close() error {
	return nil
}

// IsFull returns whether the queue is full.
func (b *InMemoryPartition) IsFull() bool {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.elements[b.writeIdx].dirty
}

// IsEmpty returns whether the queue is empty.
func (b *InMemoryPartition) IsEmpty() bool {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.elements[b.readIdx].pending || !b.elements[b.readIdx].dirty
}

func (b *InMemoryPartition) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	var errs = make([]error, len(messages))
	writeOffsets := make([]isb.Offset, len(messages))
	for idx, message := range messages {
		if !b.IsFull() {
			var err1 error
			var err2 error

			// access elements via lock
			b.rwlock.Lock()
			currentIdx := b.writeIdx
			b.elements[currentIdx].header, err1 = message.Header.MarshalBinary()
			b.elements[currentIdx].body, err2 = message.Body.MarshalBinary()
			if err1 != nil || err2 != nil {
				errs = append(errs, isb.MessageWriteErr{Name: b.name, Header: message.Header, Body: message.Body, Message: fmt.Sprintf("header:(%s) body:(%s)", err1, err2)})
			}
			errs[idx] = nil
			b.elements[currentIdx].dirty = true
			b.writeIdx = (currentIdx + 1) % b.size
			writeOffsets[idx] = isb.SimpleIntOffset(func() int64 {
				return currentIdx
			})
			// access elements via lock
			b.rwlock.Unlock()
		} else {
			switch b.options.partitionFullWritingStrategy {
			case v1alpha1.DiscardLatest:
				errs[idx] = isb.NonRetryablePartitionWriteErr{Name: b.name, Message: "Partition full!"}
			default:
				errs[idx] = isb.PartitionWriteErr{Name: b.name, Full: true, Message: "Partition full!"}
			}
		}
	}
	return writeOffsets, errs
}

func (b *InMemoryPartition) blockIfEmpty(ctx context.Context) error {
	var err error
	// block if isEmpty
	for {
		if !b.IsEmpty() {
			break
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}

	return err
}

func (b *InMemoryPartition) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	var readMessages = make([]*isb.ReadMessage, 0, count)
	cctx, cancel := context.WithTimeout(ctx, b.options.readTimeOut)
	defer cancel()
	for i := int64(0); i < count; i++ {
		// wait till we have data
		if err := b.blockIfEmpty(cctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return readMessages, nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return readMessages, nil
			}
			return readMessages, isb.PartitionReadErr{Name: b.name, Empty: true, Message: err.Error()}
		}
		// access elements via lock
		b.rwlock.Lock()

		currentIdx := b.readIdx
		// mark it as pending
		b.elements[currentIdx].pending = true
		b.readIdx = (currentIdx + 1) % b.size
		// get header and body
		header := b.elements[currentIdx].header
		body := b.elements[currentIdx].body

		b.rwlock.Unlock()

		msg, err := buildMessage(header, body)
		if err != nil {
			return readMessages, isb.MessageReadErr{
				Name:    b.name,
				Header:  header,
				Body:    body,
				Message: err.Error(),
			}
		}

		readMessage := isb.ReadMessage{Message: msg, ReadOffset: isb.SimpleIntOffset(func() int64 { return currentIdx })}

		readMessages = append(readMessages, &readMessage)
	}

	return readMessages, nil
}

func buildMessage(header []byte, body []byte) (msg isb.Message, err error) {
	err = msg.Header.UnmarshalBinary(header)
	if err != nil {
		return msg, err
	}

	err = msg.Body.UnmarshalBinary(body)
	if err != nil {
		return msg, err
	}
	return msg, err
}

// Ack acknowledges the given offsets
func (b *InMemoryPartition) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	for index, offset := range offsets {
		intOffset, err := strconv.Atoi(offset.String())
		if err != nil {
			errs[index] = isb.MessageAckErr{Name: b.name, Message: err.Error(), Offset: isb.Offset(offset)}
			continue
		}
		if int64(intOffset) >= b.size {
			errs[index] = isb.MessageAckErr{
				Name:    b.name,
				Message: fmt.Sprintf("given index (%d) >= size of the elements (%d)", intOffset, b.size),
				Offset:  offset,
			}
			continue
		}

		b.rwlock.Lock()
		b.elements[intOffset].ack = true
		b.elements[intOffset].pending = false
		b.elements[intOffset].dirty = false
		b.rwlock.Unlock()
	}

	return errs
}

func (b *InMemoryPartition) NoAck(_ context.Context, _ []isb.Offset) {}

// GetMessages gets the first num messages in the in mem partition
// this function is for testing purpose
func (b *InMemoryPartition) GetMessages(num int) []*isb.Message {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	var msgs = make([]*isb.Message, 0, num)
	for i := 0; i < num && i < len(b.elements); i++ {
		msg, _ := buildMessage(b.elements[i].header, b.elements[i].body)
		msgs = append(msgs, &msg)
	}
	return msgs
}

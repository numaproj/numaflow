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

/* package simplebuffer is in memory ring buffer that implements the isb interface. This should be used only for local development
and testing purposes. Exactly-Once is not implemented because it is a side effect. The locking implementation is very coarse.
*/

package simplebuffer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

// InMemoryBuffer implements ISB interface.
type InMemoryBuffer struct {
	name         string
	size         int64
	buffer       []elem
	writeIdx     int64
	readIdx      int64
	partitionIdx int32
	options      *options
	rwlock       *sync.RWMutex
}

func (b *InMemoryBuffer) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	//TODO implement me
	panic("implement me")
}

var _ isb.BufferReader = (*InMemoryBuffer)(nil)
var _ isb.BufferWriter = (*InMemoryBuffer)(nil)

// elem is the element stored in the buffer
type elem struct {
	header  []byte
	body    []byte
	dirty   bool
	ack     bool
	pending bool
}

// NewInMemoryBuffer returns a new buffer.
func NewInMemoryBuffer(name string, size int64, partition int32, opts ...Option) *InMemoryBuffer {

	bufferOptions := &options{
		readTimeOut:               time.Second,                // default read time out
		bufferFullWritingStrategy: v1alpha1.RetryUntilSuccess, // default buffer full writing strategy
	}

	for _, o := range opts {
		_ = o(bufferOptions)
	}

	sb := &InMemoryBuffer{
		name:         name,
		size:         size,
		buffer:       make([]elem, size),
		writeIdx:     int64(0),
		readIdx:      int64(0),
		partitionIdx: partition,
		rwlock:       new(sync.RWMutex),
		options:      bufferOptions,
	}
	return sb
}

// Stringer
func (b *InMemoryBuffer) String() string {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return fmt.Sprintf("(%s) size:%d readIdx:%d writeIdx:%d", b.name, b.size, b.readIdx, b.writeIdx)
}

// GetName returns the buffer name.
func (b *InMemoryBuffer) GetName() string {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.name
}

// GetPartitionIdx returns the partitionIdx.
func (b *InMemoryBuffer) GetPartitionIdx() int32 {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.partitionIdx
}

func (b *InMemoryBuffer) Pending(_ context.Context) (int64, error) {
	// TODO: not implemented
	return isb.PendingNotAvailable, nil
}

// Close does nothing.
func (b *InMemoryBuffer) Close() error {
	return nil
}

// IsFull returns whether the queue is full.
func (b *InMemoryBuffer) IsFull() bool {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.buffer[b.writeIdx].dirty
}

// IsEmpty returns whether the queue is empty.
func (b *InMemoryBuffer) IsEmpty() bool {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.buffer[b.readIdx].pending || !b.buffer[b.readIdx].dirty
}

func (b *InMemoryBuffer) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	var errs = make([]error, len(messages))
	writeOffsets := make([]isb.Offset, len(messages))
	for idx, message := range messages {
		if !b.IsFull() {
			var err1 error
			var err2 error

			// access buffer via lock
			b.rwlock.Lock()
			currentIdx := b.writeIdx
			b.buffer[currentIdx].header, err1 = message.Header.MarshalBinary()
			b.buffer[currentIdx].body, err2 = message.Body.MarshalBinary()
			if err1 != nil || err2 != nil {
				errs = append(errs, isb.MessageWriteErr{Name: b.name, Header: message.Header, Body: message.Body, Message: fmt.Sprintf("header:(%s) body:(%s)", err1, err2)})
			}
			errs[idx] = nil
			b.buffer[currentIdx].dirty = true
			b.writeIdx = (currentIdx + 1) % b.size
			writeOffsets[idx] = isb.NewSimpleIntPartitionOffset(currentIdx, b.partitionIdx)
			// access buffer via lock
			b.rwlock.Unlock()
		} else {
			switch b.options.bufferFullWritingStrategy {
			case v1alpha1.DiscardLatest:
				errs[idx] = isb.NonRetryableBufferWriteErr{Name: b.name, Message: isb.BufferFullMessage}
			default:
				errs[idx] = isb.BufferWriteErr{Name: b.name, Full: true, Message: isb.BufferFullMessage}
			}
		}
	}
	return writeOffsets, errs
}

func (b *InMemoryBuffer) blockIfEmpty(ctx context.Context) error {
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

func (b *InMemoryBuffer) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
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
			return readMessages, isb.BufferReadErr{Name: b.name, Empty: true, Message: err.Error()}
		}
		// access buffer via lock
		b.rwlock.Lock()

		currentIdx := b.readIdx
		// mark it as pending
		b.buffer[currentIdx].pending = true
		b.readIdx = (currentIdx + 1) % b.size
		// get header and body
		header := b.buffer[currentIdx].header
		body := b.buffer[currentIdx].body

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

		readMessage := isb.ReadMessage{Message: msg, ReadOffset: isb.NewSimpleIntPartitionOffset(currentIdx, b.partitionIdx)}

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
func (b *InMemoryBuffer) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	for index, offset := range offsets {
		intOffset, err := strconv.Atoi(strings.Split(offset.String(), "-")[0])
		if err != nil {
			errs[index] = isb.MessageAckErr{Name: b.name, Message: err.Error(), Offset: isb.Offset(offset)}
			continue
		}
		if int64(intOffset) >= b.size {
			errs[index] = isb.MessageAckErr{
				Name:    b.name,
				Message: fmt.Sprintf("given index (%d) >= size of the buffer (%d)", intOffset, b.size),
				Offset:  offset,
			}
			continue
		}

		b.rwlock.Lock()
		b.buffer[intOffset].ack = true
		b.buffer[intOffset].pending = false
		b.buffer[intOffset].dirty = false
		b.rwlock.Unlock()
	}

	return errs
}

func (b *InMemoryBuffer) NoAck(_ context.Context, _ []isb.Offset) {}

// GetMessages gets the first num messages in the in mem buffer
// this function is for testing purpose
func (b *InMemoryBuffer) GetMessages(num int) []*isb.Message {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	var msgs = make([]*isb.Message, 0, num)
	for i := 0; i < num && i < len(b.buffer); i++ {
		msg, _ := buildMessage(b.buffer[i].header, b.buffer[i].body)
		msgs = append(msgs, &msg)
	}
	return msgs
}

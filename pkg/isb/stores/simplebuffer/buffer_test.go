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

package simplebuffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

func TestNewSimpleBuffer(t *testing.T) {
	count := int64(10)
	readBatchSize := int64(2)
	sb := NewInMemoryBuffer("test", count, 0)
	ctx := context.Background()

	assert.NotEmpty(t, sb.String())
	assert.Equal(t, sb.IsEmpty(), true)

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(count, startTime, nil)
	sb.Write(ctx, writeMessages[0:5])
	assert.Equal(t, int64(5), sb.writeIdx)
	assert.Equal(t, int64(0), sb.readIdx)

	sb.Write(ctx, writeMessages[5:10])
	// 0 because 1 full iteration is done
	assert.Equal(t, int64(0), sb.writeIdx)
	assert.Equal(t, true, sb.IsFull())

	// let's read some
	readMessages, err := sb.Read(ctx, 2)
	assert.NoError(t, err)
	assert.Len(t, readMessages, int(readBatchSize))
	assert.Equal(t, []string{"0-0", "1-0"}, []string{readMessages[0].ReadOffset.String(), readMessages[1].ReadOffset.String()})
	// still full as we did not ack
	assert.Equal(t, true, sb.IsFull())

	err = sb.Ack(ctx, []isb.Offset{isb.SimpleStringOffset(func() string { return "not_a_number" })})[0]
	assert.Error(t, err)
	err = sb.Ack(ctx, []isb.Offset{isb.SimpleStringOffset(func() string { return "1000" })})[0]
	assert.Error(t, err)

	errs := sb.Ack(ctx, []isb.Offset{readMessages[0].ReadOffset, readMessages[1].ReadOffset})
	assert.NoError(t, errs[0])
	assert.NoError(t, errs[1])
	// it should no longer be full, we have 2 space left
	assert.Equal(t, false, sb.IsFull())

	// try to write 3 messages and it should fail (we have only space for 2)
	_, errs3 := sb.Write(ctx, writeMessages[0:3])
	assert.EqualValues(t, []error{nil, nil, isb.BufferWriteErr{Name: "test", Full: true, Message: isb.BufferFullMessage}}, errs3)

	// let's read some more
	readMessages, err = sb.Read(ctx, 2)
	assert.NoError(t, err)
	assert.Len(t, readMessages, int(readBatchSize))
	assert.Equal(t, []string{"2-0", "3-0"}, []string{readMessages[0].ReadOffset.String(), readMessages[1].ReadOffset.String()})
	// still full as we did not ack
	assert.Equal(t, true, sb.IsFull())
}

func TestNewSimpleBuffer_BufferFullWritingStrategyIsDiscard(t *testing.T) {
	count := int64(3)
	sb := NewInMemoryBuffer("test", 2, 0, WithBufferFullWritingStrategy(v1alpha1.DiscardLatest))
	ctx := context.Background()
	assert.NotEmpty(t, sb.String())
	assert.Equal(t, sb.IsEmpty(), true)

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(count, startTime, nil)

	// try to write 3 messages, it should fail (we have only space for 2)
	// the first 2 messages should be written, the last one should be discarded and returns us a NoRetryableError.
	_, errors := sb.Write(ctx, writeMessages[0:3])
	assert.NoError(t, errors[0])
	assert.NoError(t, errors[1])
	assert.EqualValues(t, []error{nil, nil, isb.NonRetryableBufferWriteErr{Name: "test", Message: isb.BufferFullMessage}}, errors)

	// still full as we did not ack
	assert.Equal(t, true, sb.IsFull())
}

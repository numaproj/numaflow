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

package shuffle

import (
	"fmt"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShuffle_ShuffleMessages(t *testing.T) {

	// buffer id list of test 1
	bufferIdListOne := []string{
		"buffer-1",
		"buffer-2",
		"buffer-3",
		"buffer-4",
	}

	// buffer id list for test 2
	var bufferIdListTwo []string

	isbCount := 100
	for i := 0; i < isbCount; i++ {
		bufferIdListTwo = append(bufferIdListTwo, fmt.Sprintf("buffer-%d", i+1))
	}

	// build test messages for test 1
	messagesOne := testutils.BuildTestWriteMessages(10000, time.Now())
	// set key for messagesOne
	var testMessagesOne []*isb.Message
	for index := 0; index < len(messagesOne); index++ {
		messagesOne[index].Key = fmt.Sprintf("key_%d", index)
		testMessagesOne = append(testMessagesOne, &messagesOne[index])
	}

	// build test messages for test 2
	messagesTwo := testutils.BuildTestWriteMessages(10, time.Now())
	// set key for messages
	var testMessagesTwo []*isb.Message
	for index := 0; index < len(messagesTwo); index++ {
		messagesOne[index].Key = fmt.Sprintf("key_%d", index)
		testMessagesOne = append(testMessagesOne, &messagesTwo[index])
	}

	tests := []struct {
		name               string
		buffersIdentifiers []string
		messages           []*isb.Message
	}{
		{
			name:               "MessageCountGreaterThanBufferCount",
			buffersIdentifiers: bufferIdListOne,
			messages:           testMessagesOne,
		},
		{
			name:               "BufferCountGreaterThanMessageCount",
			buffersIdentifiers: bufferIdListTwo,
			messages:           testMessagesTwo,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create shuffle with buffer id list
			shuffler := NewShuffle(test.buffersIdentifiers)

			bufferIdMessageMap := shuffler.ShuffleMessages(test.messages)
			sum := 0
			for _, value := range bufferIdMessageMap {
				sum += len(value)
			}

			assert.Equal(t, sum, len(test.messages))
		})
	}
}

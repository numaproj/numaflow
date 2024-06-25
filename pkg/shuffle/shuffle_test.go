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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

func TestShuffle_ShuffleMessages(t *testing.T) {
	tests := []struct {
		name         string
		buffersCount int
		messages     []*isb.Message
	}{
		{
			name:         "MessageCountGreaterThanBufferCount",
			buffersCount: 10,
			messages:     buildTestMessagesWithDistinctKeys(100),
		},
		{
			name:         "BufferCountGreaterThanMessageCount",
			buffersCount: 10,
			messages:     buildTestMessagesWithDistinctKeys(10),
		},
		{
			name:         "BufferCountEqualToMessageCount",
			buffersCount: 10,
			messages:     buildTestMessagesWithDistinctKeys(100),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create shuffle with buffer id list
			shuffler := NewShuffle(test.name, test.buffersCount)

			bufferIdMessageMap := shuffler.ShuffleMessages(test.messages)
			sum := 0
			for _, value := range bufferIdMessageMap {
				sum += len(value)
			}

			assert.Equal(t, sum, len(test.messages))
		})
	}
}

func TestShuffler_UseVertexNameAsSeed(t *testing.T) {
	tests := []struct {
		name                   string
		buffersCount           int
		messages               []*isb.Message
		vertexName1            string
		vertexName2            string
		expectSameDistribution bool
	}{
		{
			name:                   "MessagesDistributionRemainUnchangedWhenVertexNamesAreTheSame",
			buffersCount:           10,
			messages:               buildTestMessagesWithDistinctKeys(100),
			vertexName1:            "v1",
			vertexName2:            "v1",
			expectSameDistribution: true,
		},
		{
			name:                   "MessagesDistributionChangesWhenVertexNameChanges",
			buffersCount:           10,
			messages:               buildTestMessagesWithDistinctKeys(100),
			vertexName1:            "v1",
			vertexName2:            "v2",
			expectSameDistribution: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shuffler1 := NewShuffle(test.vertexName1, test.buffersCount)
			shuffler2 := NewShuffle(test.vertexName2, test.buffersCount)
			bufferIdMessageMap1 := shuffler1.ShuffleMessages(test.messages)
			bufferIdMessageMap2 := shuffler2.ShuffleMessages(test.messages)
			assert.Equal(t, test.expectSameDistribution, isSameShuffleDistribution(bufferIdMessageMap1, bufferIdMessageMap2))
		})
	}
}

// isSameShuffleDistribution performs a simple count check to ensure that the two input maps have the same distribution of elements.
// For a more strict verification, one could compare the contents of the two distributions, which would require sorting the elements.
func isSameShuffleDistribution(a, b map[int32][]*isb.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for key, list := range a {
		if len(list) != len(b[key]) {
			return false
		}
	}
	return true
}

func buildTestMessagesWithDistinctKeys(size int64) []*isb.Message {
	// build test messages
	messages := testutils.BuildTestWriteMessages(size, time.Now(), nil, "testVertex")
	// set keys for test messages
	var res []*isb.Message
	for index := 0; index < len(messages); index++ {
		messages[index].Keys = []string{fmt.Sprintf("key_%d", index)}
		res = append(res, &messages[index])
	}
	return res
}

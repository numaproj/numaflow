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

package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/window"
)

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int64
}

// BuildTestWriteMessages builds test isb.Message which can be used for testing.
func BuildTestWriteMessages(count int64, startTime time.Time, keys []string) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := int64(0); i < count; i++ {
		tmpTime := startTime.Add(time.Duration(i) * time.Second)
		result, _ := json.Marshal(PayloadForTest{
			Key:   fmt.Sprintf("paydload_%d", i),
			Value: i,
		})
		messages = append(messages,
			isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: tmpTime,
					},
					ID:   fmt.Sprintf("%d-testVertex-0-0", i), // TODO: hard coded ID suffix ATM, make configurable if needed
					Keys: keys,
					Headers: map[string]string{
						"key1": "value1",
						"key2": "value2",
					},
				},
				Body: isb.Body{Payload: result},
			},
		)
	}
	return messages
}

// BuildTestWindowRequests builds test window.TimedWindowRequest which can be used for testing.
func BuildTestWindowRequests(count int64, startTime time.Time, windowOp window.Operation) []window.TimedWindowRequest {
	var readMessages = BuildTestReadMessages(count, startTime, nil)
	var windowRequests = make([]window.TimedWindowRequest, count)

	for idx, readMessage := range readMessages {
		windowRequests[idx] = window.TimedWindowRequest{
			ReadMessage: &readMessage,
			Operation:   windowOp,
		}
	}
	return windowRequests
}

// BuildTestReadMessages builds test isb.ReadMessage which can be used for testing.
func BuildTestReadMessages(count int64, startTime time.Time, keys []string) []isb.ReadMessage {
	writeMessages := BuildTestWriteMessages(count, startTime, keys)
	var readMessages = make([]isb.ReadMessage, count)

	for idx, writeMessage := range writeMessages {
		readMessages[idx] = isb.ReadMessage{
			Message:    writeMessage,
			ReadOffset: isb.NewSimpleStringPartitionOffset(fmt.Sprintf("%d", idx), 0),
		}
	}

	return readMessages
}

// BuildTestReadMessagesIntOffset builds test isb.ReadMessage which can be used for testing.
func BuildTestReadMessagesIntOffset(count int64, startTime time.Time, keys []string) []isb.ReadMessage {
	writeMessages := BuildTestWriteMessages(count, startTime, keys)
	var readMessages = make([]isb.ReadMessage, count)

	for idx, writeMessage := range writeMessages {
		splitStr := strings.Split(writeMessage.Header.ID, "-")
		offset, _ := strconv.Atoi(splitStr[0])
		readMessages[idx] = isb.ReadMessage{
			Message:    writeMessage,
			ReadOffset: isb.NewSimpleIntPartitionOffset(int64(offset), 0),
			Watermark:  writeMessage.EventTime,
		}
	}

	return readMessages
}

// ReadMessagesLen is used to test the length of the messages read as they arrive on the stream
// If a stream already has 5 elements which have been read and then we add another set of elements of 10 the total number would be 15.
func ReadMessagesLen(ctx context.Context, options *redis.UniversalOptions, streamName string, expectedValue int64) bool {
	var client = redis.NewUniversalClient(options)
	var result int64
	var err error
	result, err = client.XLen(ctx, streamName).Result()
	if err != nil {
		logging.FromContext(ctx).Fatalf("error while reading stream: %s", err)
	}
	if result == expectedValue {
		return true
	}
	for result != expectedValue {
		if err != nil {
			logging.FromContext(ctx).Fatalf("error while reading stream: %s: %s", streamName, err)
		}
		result, err = client.XLen(ctx, streamName).Result()
		if result == expectedValue {
			return true
		}
		select {
		case <-ctx.Done():
			logging.FromContext(ctx).Fatalf("context error: %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	return false
}

package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int64
}

// BuildTestWriteMessages builds test isb.Message which can be used for testing.
func BuildTestWriteMessages(count int64, startTime time.Time) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := int64(0); i < count; i++ {
		tmpTime := startTime.Add(time.Duration(i) * time.Minute)
		result, _ := json.Marshal(PayloadForTest{
			Key:   fmt.Sprintf("paydload_%d", i),
			Value: i,
		})
		messages = append(messages,
			isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: tmpTime,
					},
					ID: fmt.Sprintf("%d", i),
				},
				Body: isb.Body{Payload: result},
			},
		)
	}

	return messages
}

// BuildTestReadMessages builds test isb.ReadMessage which can be used for testing.
func BuildTestReadMessages(count int64, startTime time.Time) []isb.ReadMessage {
	writeMessages := BuildTestWriteMessages(count, startTime)
	var readMessages = make([]isb.ReadMessage, count)

	for idx, writeMessage := range writeMessages {
		readMessages[idx] = isb.ReadMessage{
			Message:    writeMessage,
			ReadOffset: isb.SimpleStringOffset(func() string { return fmt.Sprintf("read_%s", writeMessage.Header.ID) }),
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

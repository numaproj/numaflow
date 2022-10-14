package reduce

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/stretchr/testify/assert"
)

type EventTypeWMProgressor struct {
	eventTime time.Time
	duration  time.Duration
}

func (e EventTypeWMProgressor) PublishWatermark(watermark processor.Watermark, offset isb.Offset) {

}

func (e EventTypeWMProgressor) GetLatestWatermark() processor.Watermark {
	return processor.Watermark{}
}

func (e EventTypeWMProgressor) StopPublisher() {
}

func (e EventTypeWMProgressor) GetWatermark(_ isb.Offset) processor.Watermark {
	return processor.Watermark(time.Now())
}

func (e EventTypeWMProgressor) GetHeadWatermark() processor.Watermark {
	return processor.Watermark{}
}

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int
}

type forwardReduceTest struct {
}

// Reduce returns a result, with last message's event time
func (f forwardReduceTest) Reduce(ctx context.Context, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	count := 0
	for range messageStream {
		count += 1
	}

	payload := PayloadForTest{Key: "count", Value: count}
	b, _ := json.Marshal(payload)
	ret := &isb.Message{
		Header: isb.Header{
			PaneInfo: isb.PaneInfo{
				EventTime: time.Now(),
			},
			ID:  "msgID",
			Key: "result",
		},
		Body: isb.Body{Payload: b},
	}
	return []*isb.Message{
		ret,
	}, nil
}

func (f forwardReduceTest) WhereTo(s string) ([]string, error) {
	return []string{"to"}, nil
}

// read from simple buffer
// mock reduce op to return result
// assert to check if the result is forwarded to toBuffers
func TestDataForward_Start(t *testing.T) {
	parentCtx := context.Background()
	child, _ := context.WithTimeout(parentCtx, time.Duration(3*time.Second))

	fromBuffer := simplebuffer.NewInMemoryBuffer("from", 1000)
	to := simplebuffer.NewInMemoryBuffer("to", 10)

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "test-1", fromBuffer)

	toBuffer := map[string]isb.BufferWriter{
		"to": to,
	}

	var err error
	var pbqManager *pbq.Manager

	// create pbqManager
	pbqManager, err = pbq.NewManager(child, pbq.WithPBQStoreOptions(store.WithStoreSize(100), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	publisher := map[string]publish.Publisher{
		"to": EventTypeWMProgressor{},
	}

	// window of 10 seconds, so that all the messages fall in the same window
	window := fixed.NewFixed(2 * time.Second)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(child, forwardReduceTest{}, fromBuffer, toBuffer, pbqManager, forwardReduceTest{}, EventTypeWMProgressor{}, publisher, window, WithReadBatchSize(10))
	assert.NoError(t, err)

	reduceDataForwarder.Start(child)

	assert.False(t, to.IsEmpty())
	msgs, readErr := to.Read(parentCtx, 1)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 1)

	// TODO assert the output of reduce (count of messages)

}

func writeMessages(ctx context.Context, count int, key string, fromBuffer *simplebuffer.InMemoryBuffer) {

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			// build 10 messages from the start time, time difference between the messages is 1 second
			messages := buildMessagesForReduce(count, key)

			// write the messages to fromBuffer, so that it will be available for consuming
			fromBuffer.Write(ctx, messages)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}

}

// buildMessagesForReduce builds test isb.Message which can be used for testing reduce.
func buildMessagesForReduce(count int, key string) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := 0; i < count; i++ {
		tmpTime := time.Now().Truncate(10 * time.Second)
		result, _ := json.Marshal(PayloadForTest{
			Key:   key,
			Value: i,
		})
		messages = append(messages,
			isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: tmpTime,
					},
					ID:  fmt.Sprintf("%d", i),
					Key: key,
				},
				Body: isb.Body{Payload: result},
			},
		)
	}

	return messages
}

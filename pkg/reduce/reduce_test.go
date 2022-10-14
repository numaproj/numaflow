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
	Value int64
}

type forwardReduceTest struct {
}

// Reduce returns a result, with last message's event time
func (f forwardReduceTest) Reduce(ctx context.Context, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	var t time.Time
	for msg := range messageStream {
		t = msg.EventTime
	}
	msg := buildMessagesForReduce(1, t, "result")
	return []*isb.Message{
		&msg[0],
	}, nil
}

func (f forwardReduceTest) WhereTo(s string) ([]string, error) {
	return []string{"to"}, nil
}

// read from simple buffer
// mock reduce op to return result
// assert to check if the result is forwarded to toBuffers
func TestDataForward_Start(t *testing.T) {
	ctx := context.Background()
	fromBuffer := simplebuffer.NewInMemoryBuffer("from", 10)
	to := simplebuffer.NewInMemoryBuffer("to", 10)

	startTime := time.Now()

	// build 10 messages from the start time, time difference between the messages is 1 second
	messages := buildMessagesForReduce(10, startTime, "test-1")

	// write the messages to fromBuffer, so that it will be available for consuming
	fromBuffer.Write(ctx, messages)

	toBuffer := map[string]isb.BufferWriter{
		"to": to,
	}

	var err error
	var pbqManager *pbq.Manager

	// create pbqManager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(100), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	publisher := map[string]publish.Publisher{
		"to": EventTypeWMProgressor{},
	}

	// window of 60 seconds, so that all the messages fall in the same window
	window := fixed.NewFixed(60 * time.Second)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(ctx, forwardReduceTest{}, fromBuffer, toBuffer, pbqManager, forwardReduceTest{}, EventTypeWMProgressor{}, publisher, window, WithReadBatchSize(10))
	assert.NoError(t, err)

	reduceDataForwarder.Start(ctx)
}

// buildMessagesForReduce builds test isb.Message which can be used for testing reduce.
func buildMessagesForReduce(count int64, startTime time.Time, key string) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := int64(0); i < count; i++ {
		tmpTime := startTime.Add(time.Duration(i) * time.Second)
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

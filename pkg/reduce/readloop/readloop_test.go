package readloop

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
)

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int
}

type SumReduceTest struct {
}

func (s *SumReduceTest) WhereTo(_ string) ([]string, error) {
	return []string{"reduce-buffer"}, nil
}

func (s *SumReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	sum := 0
	for msg := range messageStream {
		var payload PayloadForTest
		_ = json.Unmarshal(msg.Payload, &payload)
		sum += payload.Value
	}

	payload := PayloadForTest{Key: "sum", Value: sum}
	b, _ := json.Marshal(payload)
	ret := &isb.Message{
		Header: isb.Header{
			PaneInfo: isb.PaneInfo{
				StartTime: partitionID.Start,
				EndTime:   partitionID.End,
				EventTime: partitionID.End,
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

// testing startup code with replay included using in-memory pbq
func TestReadLoop_Startup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// partitions to be replayed
	partitionIds := []partition.ID{
		{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Key:   "even",
		},
		{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Key:   "odd",
		},
		{
			Start: time.Unix(120, 0),
			End:   time.Unix(180, 0),
			Key:   "odd",
		},
		{
			Start: time.Unix(180, 0),
			End:   time.Unix(240, 0),
			Key:   "even",
		},
	}

	memStoreProvider := memory.NewMemoryStores(memory.WithStoreSize(100))

	for _, id := range partitionIds {
		memStore, err := memStoreProvider.CreateStore(ctx, id)
		assert.NoError(t, err)

		var msgVal int
		if id.Key == "even" {
			msgVal = 2
		} else {
			msgVal = 3
		}

		// write messages to the store, which will be replayed
		storeMessages := createStoreMessages(ctx, id.Key, msgVal, id.Start, 10)
		for _, msg := range storeMessages {
			err = memStore.Write(msg)
			assert.NoError(t, err)
		}
	}

	pManager, _ := pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memStoreProvider, pbq.WithChannelBufferSize(10))

	to1 := simplebuffer.NewInMemoryBuffer("reduce-buffer", 4)
	toSteps := map[string]isb.BufferWriter{
		"reduce-buffer": to1,
	}

	_, pw := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

	window := fixed.NewFixed(60 * time.Second)

	rl, err := NewReadLoop(ctx, "reduce", "test-pipeline", 0, &SumReduceTest{}, pManager, window, toSteps, &SumReduceTest{}, pw)
	assert.NoError(t, err)
	err = rl.Startup(ctx)
	assert.NoError(t, err)

	// send a message with the higher watermark so that the windows will be closed
	latestMessage := &isb.ReadMessage{
		Message: isb.Message{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{
					EventTime: time.Unix(300, 0),
					StartTime: time.Time{},
					EndTime:   time.Time{},
					IsLate:    false,
				},
				ID:  "",
				Key: "",
			},
			Body: isb.Body{},
		},
		ReadOffset: isb.SimpleStringOffset(func() string { return "simple-offset" }),
		Watermark:  time.Unix(300, 0),
	}

	rl.Process(ctx, []*isb.ReadMessage{latestMessage})
	for !to1.IsFull() {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	msgs, readErr := to1.Read(ctx, 4)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 4)

	// since we have 3 partitions we should have 3 different outputs
	var readMessagePayload1 PayloadForTest
	var readMessagePayload2 PayloadForTest
	var readMessagePayload3 PayloadForTest
	var readMessagePayload4 PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload1)
	_ = json.Unmarshal(msgs[1].Payload, &readMessagePayload2)
	_ = json.Unmarshal(msgs[2].Payload, &readMessagePayload3)
	_ = json.Unmarshal(msgs[3].Payload, &readMessagePayload4)
	// since we had 10 messages in the store with value 2 and 3
	// the expected value is 20 and 30, since the reduce operation is sum
	assert.Contains(t, []int{20, 30, 20, 30}, readMessagePayload1.Value)
	assert.Contains(t, []int{20, 30, 20, 30}, readMessagePayload2.Value)
	assert.Contains(t, []int{20, 30, 20, 30}, readMessagePayload3.Value)
	assert.Contains(t, []int{20, 30, 20, 30}, readMessagePayload4.Value)
	assert.Equal(t, "sum", readMessagePayload1.Key)
	assert.Equal(t, "sum", readMessagePayload2.Key)
	assert.Equal(t, "sum", readMessagePayload3.Key)
	assert.Equal(t, "sum", readMessagePayload4.Key)

}

func createStoreMessages(_ context.Context, key string, value int, eventTime time.Time, count int) []*isb.ReadMessage {
	readMessages := make([]*isb.ReadMessage, count)
	for j := 0; j < count; j++ {
		result, _ := json.Marshal(PayloadForTest{
			Key:   key,
			Value: value,
		})
		readMessage := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: eventTime,
					},
					ID:  fmt.Sprintf("%d", value+1),
					Key: key,
				},
				Body: isb.Body{Payload: result},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "simple-offset" }),
		}
		eventTime = eventTime.Add(time.Second)
		readMessages[j] = readMessage
	}
	return readMessages
}

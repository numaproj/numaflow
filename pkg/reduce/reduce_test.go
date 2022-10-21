package reduce

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/stretchr/testify/assert"
)

type EventTypeWMProgressor struct{}

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

type CounterReduceTest struct {
}

// Reduce returns a result with the count of messages
func (f CounterReduceTest) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	count := 0
	for range messageStream {
		count += 1
	}

	payload := PayloadForTest{Key: "count", Value: count}
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

func (f CounterReduceTest) WhereTo(s string) ([]string, error) {
	return []string{"to"}, nil
}

type SumReduceTest struct {
}

func (s SumReduceTest) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
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

type MaxReduceTest struct {
}

func (m MaxReduceTest) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	mx := math.MinInt64
	for msg := range messageStream {
		var payload PayloadForTest
		_ = json.Unmarshal(msg.Payload, &payload)
		if payload.Value > mx {
			mx = payload.Value
		}
	}

	payload := PayloadForTest{Key: "max", Value: mx}
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

// read from simple buffer
// mock reduce op to return result
// assert to check if the result is forwarded to toBuffers
func TestDataForward_StartWithNoOpWM(t *testing.T) {
	windowTime := 2 * time.Second
	parentCtx := context.Background()
	child, cancelFn := context.WithTimeout(parentCtx, windowTime*2)
	defer cancelFn()
	fromBufferSize := int64(1000)
	toBufferSize := int64(10)
	fromBuffer := simplebuffer.NewInMemoryBuffer("from", fromBufferSize)
	to := simplebuffer.NewInMemoryBuffer("to", toBufferSize)

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "no-op-test", fromBuffer, EventTypeWMProgressor{}, time.Second*1)

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

	// create new fixed window of (windowTime)
	window := fixed.NewFixed(windowTime)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(child, CounterReduceTest{}, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, EventTypeWMProgressor{}, publisher, window, WithReadBatchSize(10))
	assert.NoError(t, err)

	go reduceDataForwarder.Start(child)

	for to.IsEmpty() {
		select {
		case <-child.Done():
			assert.Fail(t, child.Err().Error())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := to.Read(child, 1)

	assert.Nil(t, readErr)
	assert.Len(t, msgs, 1)

	// assert the output of reduce (count of messages)
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// here the count will always be equal to the number of messages written per key
	assert.Equal(t, 1, readMessagePayload.Value)
	assert.Equal(t, "count", readMessagePayload.Key)

}

func TestDataForward_StartWithInMemoryWMStore(t *testing.T) {
	var err error
	fromBufferSize := int64(100000)
	toBufferSize := int64(10)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	//create from buffers for tests
	fromBuffer1 := simplebuffer.NewInMemoryBuffer("from1", fromBufferSize)
	fromBuffer2 := simplebuffer.NewInMemoryBuffer("from2", fromBufferSize)
	fromBuffer3 := simplebuffer.NewInMemoryBuffer("from3", fromBufferSize)

	//create to buffers for tests
	buffer1 := simplebuffer.NewInMemoryBuffer("to", toBufferSize)
	buffer2 := simplebuffer.NewInMemoryBuffer("to", toBufferSize)
	buffer3 := simplebuffer.NewInMemoryBuffer("to", toBufferSize)
	toBuffer1 := map[string]isb.BufferWriter{
		"to": buffer1,
	}
	toBuffer2 := map[string]isb.BufferWriter{
		"to": buffer2,
	}
	toBuffer3 := map[string]isb.BufferWriter{
		"to": buffer3,
	}

	// create pbqManager for tests
	var pbqManager1, pbqManager2, pbqManager3 *pbq.Manager
	pbqManager1, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(100), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	pbqManager2, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(100), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	pbqManager3, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(10000), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create watermark fetcher and publisher for tests
	f1, p1 := fetcherAndPublisher(ctx, toBuffer1, fromBuffer1, "test-1")
	f2, p2 := fetcherAndPublisher(ctx, toBuffer2, fromBuffer2, "test-2")
	f3, p3 := fetcherAndPublisher(ctx, toBuffer3, fromBuffer3, "test-3")

	// create go routine sources which produces messages
	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(ctx, 10, "test-1", fromBuffer1, p1["from1"], time.Second*2)
	go writeMessages(ctx, 100, "test-2", fromBuffer2, p2["from2"], time.Minute*1)
	go writeMessages(ctx, 1000, "test-3", fromBuffer3, p3["from3"], time.Minute*10)

	//create window for tests
	window1 := fixed.NewFixed(2 * time.Second)
	window2 := fixed.NewFixed(2 * time.Minute)
	window3 := fixed.NewFixed(20 * time.Minute)

	//create forwarder for tests
	var reduceDataForwarder1, reduceDataForwarder2, reduceDataForwarder3 *DataForward
	reduceDataForwarder1, err = NewDataForward(ctx, CounterReduceTest{}, fromBuffer1, toBuffer1, pbqManager1, CounterReduceTest{}, f1, p1, window1, WithReadBatchSize(10))
	assert.NoError(t, err)
	reduceDataForwarder2, err = NewDataForward(ctx, SumReduceTest{}, fromBuffer2, toBuffer2, pbqManager2, CounterReduceTest{}, f2, p2, window2, WithReadBatchSize(10))
	assert.NoError(t, err)
	reduceDataForwarder3, err = NewDataForward(ctx, MaxReduceTest{}, fromBuffer3, toBuffer3, pbqManager3, CounterReduceTest{}, f3, p3, window3, WithReadBatchSize(10))
	assert.NoError(t, err)

	tests := []struct {
		name                string
		window              *fixed.Fixed
		fromBuffer          isb.BufferReader
		toBuffer            *simplebuffer.InMemoryBuffer
		reduceDataForwarder *DataForward
		expectedKey         string
		expectedValue       int64
	}{
		{
			name:                "test-1",
			window:              window1,
			fromBuffer:          fromBuffer1,
			toBuffer:            buffer1,
			reduceDataForwarder: reduceDataForwarder1,
			expectedKey:         "count",
			expectedValue:       10,
		},
		{
			name:                "test-2",
			window:              window2,
			fromBuffer:          fromBuffer2,
			toBuffer:            buffer2,
			reduceDataForwarder: reduceDataForwarder2,
			expectedKey:         "sum",
			expectedValue:       4950,
		},
		{
			name:                "test-3",
			window:              window3,
			fromBuffer:          fromBuffer3,
			toBuffer:            buffer3,
			reduceDataForwarder: reduceDataForwarder3,
			expectedKey:         "max",
			expectedValue:       999,
		},
	}

	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			go value.reduceDataForwarder.Start(ctx)

			// wait until there is data in to buffer
			for value.toBuffer.IsEmpty() {
				select {
				case <-ctx.Done():
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}

			// we are reading only one message here but the count should be equal to
			// the number of keyed windows that closed
			msgs, readErr := value.toBuffer.Read(ctx, 1)
			assert.Nil(t, readErr)
			assert.Len(t, msgs, 1)

			// assert the output of reduce
			var readMessagePayload PayloadForTest
			_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
			assert.Equal(t, value.expectedValue, int64(readMessagePayload.Value))
			assert.Equal(t, value.expectedKey, readMessagePayload.Key)
		})
	}

}

// fetcherAndPublisher creates watermark fetcher and publishers for toBuffers, and keeps the processors alive by sending heartbeats
func fetcherAndPublisher(ctx context.Context, toBuffers map[string]isb.BufferWriter, fromBuffer *simplebuffer.InMemoryBuffer, key string) (fetch.Fetcher, map[string]publish.Publisher) {

	var (
		keyspace     = key
		pipelineName = "testPipeline"
		hbBucketName = keyspace + "_PROCESSORS"
		otBucketName = keyspace + "_OT"
	)

	publishEntity := processor.NewProcessorEntity("reduceProcessor" + keyspace)
	sourcePublishEntity := processor.NewProcessorEntity("sourceProcessor" + keyspace)
	hb, hbWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	ot, otWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)

	publishers := make(map[string]publish.Publisher)

	p := publish.NewPublish(ctx, publishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
	sourcePublisher := publish.NewPublish(ctx, sourcePublishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = hb.PutKV(ctx, "sourceProcessor"+keyspace, []byte(fmt.Sprintf("%d", time.Now().Unix())))
				_ = hb.PutKV(ctx, "reduceProcessor"+keyspace, []byte(fmt.Sprintf("%d", time.Now().Unix())))
				time.Sleep(time.Duration(1) * time.Second)
			}
		}
	}()

	publishers[fromBuffer.GetName()] = sourcePublisher
	for key := range toBuffers {
		publishers[key] = p
	}

	hbWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_PROCESSORS", hbWatcherCh)
	otWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_OT", otWatcherCh)

	var pm = fetch.NewProcessorManager(ctx, wmstore.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), fetch.WithPodHeartbeatRate(1), fetch.WithRefreshingProcessorsRate(1), fetch.WithSeparateOTBuckets(false))
	var f = fetch.NewEdgeFetcher(ctx, fromBuffer.GetName(), pm)
	return f, publishers
}

// buildMessagesForReduce builds test isb.Message which can be used for testing reduce.
func buildMessagesForReduce(count int, key string, publishTime time.Time) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := 0; i < count; i++ {
		result, _ := json.Marshal(PayloadForTest{
			Key:   key,
			Value: i,
		})
		messages = append(messages,
			isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: publishTime,
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

// writeMessages writes message to simple buffer and publishes the watermark using write offsets
func writeMessages(ctx context.Context, count int, key string, fromBuffer *simplebuffer.InMemoryBuffer, publish publish.Publisher, interval time.Duration) {
	generateTime := 100 * time.Millisecond
	ticker := time.NewTicker(generateTime)
	intervalDuration := interval
	publishTime := time.Unix(60, 0)
	i := 1
	for {
		select {
		case <-ticker.C:
			// build  messages with eventTime time.Now()
			publishTime = publishTime.Add(intervalDuration)
			messages := buildMessagesForReduce(count, key+strconv.Itoa(i), publishTime)
			i++

			// write the messages to fromBuffer, so that it will be available for consuming
			offsets, _ := fromBuffer.Write(ctx, messages)
			if len(offsets) > 0 {
				publish.PublishWatermark(processor.Watermark(publishTime), offsets[len(offsets)-1])
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

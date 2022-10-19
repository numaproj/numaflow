package reduce

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/pbq"
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
	go writeMessages(child, 10, "test-1", fromBuffer, EventTypeWMProgressor{})

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
	reduceDataForwarder, err = NewDataForward(child, forwardReduceTest{}, fromBuffer, toBuffer, pbqManager, forwardReduceTest{}, EventTypeWMProgressor{}, publisher, window, WithReadBatchSize(10))
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
	assert.Equal(t, 10, readMessagePayload.Value)
	assert.Equal(t, "count", readMessagePayload.Key)

}

func TestDataForward_StartWithInMemoryWMStore(t *testing.T) {
	windowTime := 2 * time.Second
	parentCtx := context.Background()
	child, cancelFn := context.WithTimeout(parentCtx, windowTime*3)
	defer cancelFn()
	fromBufferSize := int64(1000)
	toBufferSize := int64(10)
	fromBuffer := simplebuffer.NewInMemoryBuffer("from", fromBufferSize)
	to := simplebuffer.NewInMemoryBuffer("to", toBufferSize)

	toBuffer := map[string]isb.BufferWriter{
		"to": to,
	}

	f, p := fetcherAndPublisher(toBuffer)

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "test-1", fromBuffer, p["from"])

	var err error
	var pbqManager *pbq.Manager

	// create pbqManager
	pbqManager, err = pbq.NewManager(child, pbq.WithPBQStoreOptions(store.WithStoreSize(100), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create new fixed window of (windowTime)
	window := fixed.NewFixed(windowTime)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(child, forwardReduceTest{}, fromBuffer, toBuffer, pbqManager, forwardReduceTest{}, f, p, window, WithReadBatchSize(10))
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
	assert.Equal(t, 10, readMessagePayload.Value)
	assert.Equal(t, "count", readMessagePayload.Key)

}

// fetcherAndPublisher creates watermark fetcher and publishers for toBuffers, and keeps the processors alive by sending heartbeats
func fetcherAndPublisher(toBuffers map[string]isb.BufferWriter) (fetch.Fetcher, map[string]publish.Publisher) {

	var (
		ctx          = context.Background()
		keyspace     = "reduce"
		pipelineName = "testPipeline"
		hbBucketName = keyspace + "_PROCESSORS"
		otBucketName = keyspace + "_OT"
	)

	publishEntity := processor.NewProcessorEntity("reduceProcessor")
	sourcePublishEntity := processor.NewProcessorEntity("sourceProcessor")
	hb, hbWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	ot, otWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)

	publishers := make(map[string]publish.Publisher)

	p := publish.NewPublish(ctx, publishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
	sourcePublisher := publish.NewPublish(ctx, sourcePublishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))

	go func() {
		for {
			_ = hb.PutKV(ctx, "sourceProcessor", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			_ = hb.PutKV(ctx, "reduceProcessor", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			time.Sleep(time.Duration(1) * time.Second)
		}
	}()

	publishers["from"] = sourcePublisher
	for key := range toBuffers {
		publishers[key] = p
	}

	hbWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_PROCESSORS", hbWatcherCh)
	otWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_OT", otWatcherCh)

	var pm = fetch.NewProcessorManager(ctx, wmstore.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), fetch.WithPodHeartbeatRate(1), fetch.WithRefreshingProcessorsRate(1), fetch.WithSeparateOTBuckets(false))
	var f = fetch.NewEdgeFetcher(ctx, "from", pm)
	return f, publishers
}

// buildMessagesForReduce builds test isb.Message which can be used for testing reduce.
func buildMessagesForReduce(count int, key string) []isb.Message {
	var messages = make([]isb.Message, 0, count)
	for i := 0; i < count; i++ {
		tmpTime := time.Now()
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

// writeMessages writes message to simple buffer and publishes the watermark using write offsets
func writeMessages(ctx context.Context, count int, key string, fromBuffer *simplebuffer.InMemoryBuffer, publish publish.Publisher) {
	generateTime := 1 * time.Second
	ticker := time.NewTicker(generateTime)
	i := 1
	for {
		select {
		case <-ticker.C:
			// build  messages with eventTime time.Now()
			publishTime := time.Now()
			messages := buildMessagesForReduce(count, key+strconv.Itoa(i))
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

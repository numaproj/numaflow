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
	return []string{"reduce-to-buffer"}, nil
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
	var (
		windowTime      = 2 * time.Second
		parentCtx       = context.Background()
		child, cancelFn = context.WithTimeout(parentCtx, windowTime*2)
		fromBufferSize  = int64(1000)
		toBufferSize    = int64(10)
		fromBufferName  = "source-from-buffer"
		toBufferName    = "reduce-to-buffer"
	)
	defer cancelFn()
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)
	to := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "no-op-test", fromBuffer, EventTypeWMProgressor{}, time.Second*1)

	toBuffer := map[string]isb.BufferWriter{
		toBufferName: to,
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
			return
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

// Count operation with 1 min window
func TestReduceDataForward_Count(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{7}
		startTime      = 60000 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	//create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(1000), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, toBuffer, fromBuffer, t.Name())

	// create a fixed window of 60s
	window := fixed.NewFixed(60 * time.Second)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, CounterReduceTest{}, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, p,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start(ctx)

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 300, 10, p[fromBuffer.GetName()], fromBuffer)

	// wait until there is data in to buffer
	for buffer.IsEmpty() {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 1)

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// since the window duration is 60s and tps is 1, the count should be 60
	assert.Equal(t, int64(60), int64(readMessagePayload.Value))
	assert.Equal(t, "count", readMessagePayload.Key)

}

// Sum operation with 2 minutes window
func TestReduceDataForward_Sum(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{10}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	//create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(1000), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, toBuffer, fromBuffer, t.Name())

	// create a fixed window of 2 minutes
	window := fixed.NewFixed(2 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, SumReduceTest{}, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, p,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start(ctx)

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 300, 10, p[fromBuffer.GetName()], fromBuffer)

	// wait until there is data in to buffer
	for buffer.IsEmpty() {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 1)

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// since the window duration is 2 minutes and tps is 1, the sum should be 120  * 10
	assert.Equal(t, int64(1200), int64(readMessagePayload.Value))
	assert.Equal(t, "sum", readMessagePayload.Key)

}

// Max operation with 5 minutes window
func TestReduceDataForward_Max(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{100}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	//create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(1000), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, toBuffer, fromBuffer, t.Name())

	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, MaxReduceTest{}, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, p,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start(ctx)

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 600, 10, p[fromBuffer.GetName()], fromBuffer)

	// wait until there is data in to buffer
	for buffer.IsEmpty() {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 1)

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// since all the messages have the same value the max should be 100
	assert.Equal(t, int64(100), int64(readMessagePayload.Value))
	assert.Equal(t, "max", readMessagePayload.Key)

}

// Max operation with 5 minutes window and two keys
func TestReduceDataForward_SumWithDifferentKeys(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	//create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(1000), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, toBuffer, fromBuffer, t.Name())

	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, SumReduceTest{}, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, p,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start(ctx)

	// start the producer
	go publishMessages(ctx, startTime, messages, 600, 10, p[fromBuffer.GetName()], fromBuffer)

	// wait until there is data in to buffer
	for buffer.IsEmpty() {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 2)
	assert.Nil(t, readErr)
	assert.Len(t, msgs, 2)

	// assert the output of reduce
	var readMessagePayload1 PayloadForTest
	var readMessagePayload2 PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload1)
	_ = json.Unmarshal(msgs[1].Payload, &readMessagePayload2)
	// since the window duration is 5 minutes, the output should be
	// 100 * 300(for key even) and 100 * 97(for key odd)
	// we cant guarantee the order of the output
	assert.Contains(t, []int{30000, 29700}, readMessagePayload1.Value)
	assert.Contains(t, []int{30000, 29700}, readMessagePayload2.Value)
	assert.Equal(t, "sum", readMessagePayload1.Key)
	assert.Equal(t, "sum", readMessagePayload2.Key)

}

// fetcherAndPublisher creates watermark fetcher and publishers, and keeps the processors alive by sending heartbeats
func fetcherAndPublisher(ctx context.Context, toBuffers map[string]isb.BufferWriter, fromBuffer *simplebuffer.InMemoryBuffer, key string) (fetch.Fetcher, map[string]publish.Publisher) {

	var (
		keyspace     = key
		pipelineName = "testPipeline"
		hbBucketName = keyspace + "_PROCESSORS"
		otBucketName = keyspace + "_OT"
	)

	sourcePublishEntity := processor.NewProcessorEntity(fromBuffer.GetName())
	hb, hbWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	ot, otWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)

	publishers := make(map[string]publish.Publisher)

	// publisher for source
	sourcePublisher := publish.NewPublish(ctx, sourcePublishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))

	// create publisher for to Buffers
	for key := range toBuffers {
		publishEntity := processor.NewProcessorEntity(key)
		p := publish.NewPublish(ctx, publishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	publishers[fromBuffer.GetName()] = sourcePublisher

	// publish heartbeat for the processors
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				for key := range publishers {
					_ = hb.PutKV(ctx, key, []byte(fmt.Sprintf("%d", time.Now().Unix())))
				}
				time.Sleep(time.Duration(1) * time.Second)
			}
		}
	}()

	hbWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_PROCESSORS", hbWatcherCh)
	otWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_OT", otWatcherCh)

	// fetcher for reduce
	var pm = fetch.NewProcessorManager(ctx, wmstore.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), fetch.WithPodHeartbeatRate(1))
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

func publishMessages(ctx context.Context, startTime int, messages []int, testDuration int, batchSize int, publish publish.Publisher, fromBuffer *simplebuffer.InMemoryBuffer) {
	eventTime := startTime

	inputChan := make(chan int)

	go func() {
		for i := 0; i < testDuration; i++ {
			inputChan <- eventTime + (i * 1000)
		}
		close(inputChan)
	}()

	inputMsgs := make([]isb.Message, batchSize)
	count := 0

	for {
		select {
		case et, ok := <-inputChan:
			if !ok {
				return
			}

			for _, message := range messages {
				inputMsg := buildIsbMessage(message, time.UnixMilli(int64(et)))
				inputMsgs[count] = inputMsg
				count += 1

				if count >= batchSize {
					offsets, _ := fromBuffer.Write(ctx, inputMsgs)
					if len(offsets) > 0 {
						publish.PublishWatermark(processor.Watermark(time.UnixMilli(int64(et))), offsets[len(offsets)-1])
					}
					count = 0
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func buildIsbMessage(messageValue int, eventTime time.Time) isb.Message {
	var messageKey string
	if messageValue%2 == 0 {
		messageKey = "even"
	} else {
		messageKey = "odd"
	}

	result, _ := json.Marshal(PayloadForTest{
		Key:   messageKey,
		Value: messageValue,
	})
	return isb.Message{
		Header: isb.Header{
			PaneInfo: isb.PaneInfo{
				EventTime: eventTime,
			},
			ID:  fmt.Sprintf("%d", messageValue),
			Key: messageKey,
		},
		Body: isb.Body{Payload: result},
	}
}

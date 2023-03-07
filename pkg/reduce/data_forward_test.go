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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
)

var keyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			UDF:  &dfv1.UDF{GroupBy: &dfv1.GroupBy{Keyed: true}},
		},
	}},
	Hostname: "test-host",
	Replica:  0,
}

var nonKeyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			UDF:  &dfv1.UDF{GroupBy: &dfv1.GroupBy{Keyed: false}},
		},
	}},
	Hostname: "test-host",
	Replica:  0,
}

type EventTypeWMProgressor struct {
	watermarks map[string]wmb.Watermark
	m          sync.Mutex
}

func (e *EventTypeWMProgressor) PublishWatermark(watermark wmb.Watermark, offset isb.Offset) {
	e.m.Lock()
	defer e.m.Unlock()
	e.watermarks[offset.String()] = watermark
}

func (e *EventTypeWMProgressor) PublishIdleWatermark(wmb.Watermark) {
	// TODO
}

func (e *EventTypeWMProgressor) GetLatestWatermark() wmb.Watermark {
	return wmb.Watermark{}
}

func (e *EventTypeWMProgressor) Close() error {
	return nil
}

func (e *EventTypeWMProgressor) GetWatermark(offset isb.Offset) wmb.Watermark {
	e.m.Lock()
	defer e.m.Unlock()
	return e.watermarks[offset.String()]
}

func (e *EventTypeWMProgressor) GetHeadWatermark() wmb.Watermark {
	return wmb.Watermark{}
}

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int
}

type CounterReduceTest struct {
}

// Reduce returns a result with the count of messages
func (f CounterReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	count := 0
	for range messageStream {
		count += 1
	}

	payload := PayloadForTest{Key: "count", Value: count}
	b, _ := json.Marshal(payload)
	ret := &isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{
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

func (f CounterReduceTest) WhereTo(_ string) ([]string, error) {
	return []string{"reduce-to-buffer"}, nil
}

type SumReduceTest struct {
}

func (s SumReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	sums := make(map[string]int)

	for msg := range messageStream {
		var payload PayloadForTest
		_ = json.Unmarshal(msg.Payload, &payload)
		key := msg.Key
		sums[key] += payload.Value
	}

	msgs := make([]*isb.Message, 0)

	for k, s := range sums {
		payload := PayloadForTest{Key: k, Value: s}
		b, _ := json.Marshal(payload)
		msg := &isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: partitionID.End,
				},
				ID:  "msgID",
				Key: k,
			},
			Body: isb.Body{Payload: b},
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

type MaxReduceTest struct {
}

func (m MaxReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	mx := math.MinInt64
	maxMap := make(map[string]int)
	for msg := range messageStream {
		var payload PayloadForTest
		_ = json.Unmarshal(msg.Payload, &payload)
		if max, ok := maxMap[msg.Key]; ok {
			mx = max
		}
		if payload.Value > mx {
			mx = payload.Value
			maxMap[msg.Key] = mx
		}
	}

	result := make([]*isb.Message, 0)
	for k, max := range maxMap {
		payload := PayloadForTest{Key: k, Value: max}
		b, _ := json.Marshal(payload)
		ret := &isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: partitionID.End,
				},
				ID:  "msgID",
				Key: k,
			},
			Body: isb.Body{Payload: b},
		}

		result = append(result, ret)
	}

	return result, nil
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

	wmpublisher := &EventTypeWMProgressor{
		watermarks: make(map[string]wmb.Watermark),
	}

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "no-op-test", fromBuffer, wmpublisher, time.Second*1)

	toBuffer := map[string]isb.BufferWriter{
		toBufferName: to,
	}

	var err error
	var pbqManager *pbq.Manager

	// create pbqManager
	pbqManager, err = pbq.NewManager(child, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(100)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	publisher := map[string]publish.Publisher{
		"to": &EventTypeWMProgressor{},
	}

	// create new fixed window of (windowTime)
	window := fixed.NewFixed(windowTime)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(child, CounterReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, wmpublisher, publisher,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	go reduceDataForwarder.Start()

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
	assert.Equal(t, 10, readMessagePayload.Value)
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
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, memory.NewMemoryStores(memory.WithStoreSize(1000)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publisherMap := createPublisherForBuffer(ctx, toBuffer, pipelineName)

	// create a fixed window of 60s
	window := fixed.NewFixed(60 * time.Second)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, CounterReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publisherMap,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 300, 10, p, fromBuffer)

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
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, memory.NewMemoryStores(memory.WithStoreSize(1000)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap := createPublisherForBuffer(ctx, toBuffer, pipelineName)
	// create a fixed window of 2 minutes
	window := fixed.NewFixed(2 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, SumReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publishersMap,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	publishMessages(ctx, startTime, messageValue, 300, 10, p, fromBuffer)

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
	assert.Equal(t, "even", readMessagePayload.Key)

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
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, memory.NewMemoryStores(memory.WithStoreSize(1000)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap := createPublisherForBuffer(ctx, toBuffer, pipelineName)

	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, MaxReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publishersMap,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 600, 10, p, fromBuffer)

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
	assert.Equal(t, "even", readMessagePayload.Key)

}

// Max operation with 5 minutes window and two keys
func TestReduceDataForward_SumWithDifferentKeys(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(1000)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap := createPublisherForBuffer(ctx, toBuffer, pipelineName)
	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward

	reduceDataForward, err = NewDataForward(ctx, SumReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publishersMap,
		window, WithReadBatchSize(10))

	assert.NoError(t, err)

	// start the producer
	go publishMessages(ctx, startTime, messages, 650, 10, p, fromBuffer)

	// start the forwarder
	go reduceDataForward.Start()

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
	// 100 * 300(for key even) and 99 * 300(for key odd)
	// we cant guarantee the order of the output
	assert.Contains(t, []int{30000, 29700}, readMessagePayload1.Value)
	assert.Contains(t, []int{30000, 29700}, readMessagePayload2.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload1.Key)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload2.Key)

}

// Max operation with 5 minutes window and non keyed
func TestReduceDataForward_NonKeyed(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, memory.NewMemoryStores(memory.WithStoreSize(1000)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap := createPublisherForBuffer(ctx, toBuffer, pipelineName)

	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, SumReduceTest{}, nonKeyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publishersMap,
		window, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messages, 600, 10, p, fromBuffer)

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
	// since the window duration is 5 minutes, the output should be
	// 100 * 300 + 99 * 300
	// we cant guarantee the order of the output
	assert.Equal(t, 59700, readMessagePayload.Value)
	assert.Equal(t, dfv1.DefaultKeyForNonKeyedData, readMessagePayload.Key)

}

func TestDataForward_WithContextClose(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 5*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toBufferName   = "reduce-to-buffer"
		pipelineName   = "test-reduce-pipeline"
		err            error
	)

	cctx, childCancel := context.WithCancel(ctx)

	defer cancel()
	defer childCancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toBufferName, toBufferSize)
	toBuffer := map[string]isb.BufferWriter{
		toBufferName: buffer,
	}

	// create a store provider
	storeProvider := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(cctx, "reduce", "test-pipeline", 0, storeProvider,
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(cctx, fromBuffer, t.Name())
	publishersMap := createPublisherForBuffer(cctx, toBuffer, pipelineName)

	// create a fixed window of 5 minutes
	window := fixed.NewFixed(5 * time.Minute)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(cctx, SumReduceTest{}, keyedVertex, fromBuffer, toBuffer, pbqManager, CounterReduceTest{}, f, publishersMap,
		window, WithReadBatchSize(1))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()
	// window duration is 300s, we are sending only 200 messages with event time less than window end time, so the window will not be closed
	publishMessages(cctx, startTime, messages, 200, 10, p, fromBuffer)
	// wait for the partitions to be created
	for {
		partitionsList := pbqManager.ListPartitions()
		if len(partitionsList) == 1 {
			childCancel()
			break
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	var discoveredPartitions []partition.ID
	for {
		discoveredPartitions, _ = storeProvider.DiscoverPartitions(ctx)

		if len(discoveredPartitions) == 1 {
			break
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// even though we have 2 different keys
	assert.Len(t, discoveredPartitions, 1)

}

// fetcherAndPublisher creates watermark fetcher and publishers, and keeps the processors alive by sending heartbeats
func fetcherAndPublisher(ctx context.Context, fromBuffer *simplebuffer.InMemoryBuffer, key string) (fetch.Fetcher, publish.Publisher) {

	var (
		keyspace     = key
		pipelineName = "testPipeline"
		hbBucketName = keyspace + "_PROCESSORS"
		otBucketName = keyspace + "_OT"
	)

	sourcePublishEntity := processor.NewProcessorEntity(fromBuffer.GetName())
	hb, hbWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	ot, otWatcherCh, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)

	// publisher for source
	sourcePublisher := publish.NewPublish(ctx, sourcePublishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))

	// publish heartbeat for the processors
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = hb.PutKV(ctx, fromBuffer.GetName(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
				time.Sleep(time.Duration(1) * time.Second)
			}
		}
	}()

	hbWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_PROCESSORS", hbWatcherCh)
	otWatcher, _ := inmem.NewInMemWatch(ctx, pipelineName, keyspace+"_OT", otWatcherCh)

	var f = fetch.NewEdgeFetcher(ctx, fromBuffer.GetName(), wmstore.BuildWatermarkStoreWatcher(hbWatcher, otWatcher))
	return f, sourcePublisher
}

func createPublisherForBuffer(ctx context.Context, toBuffers map[string]isb.BufferWriter, pipelineName string) map[string]publish.Publisher {
	publishers := make(map[string]publish.Publisher)

	// create publisher for to Buffers
	for key := range toBuffers {
		publishEntity := processor.NewProcessorEntity(key)
		hb, _, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, key+"_PROCESSORS")
		ot, _, _ := inmem.NewKVInMemKVStore(ctx, pipelineName, key+"_OT")
		p := publish.NewPublish(ctx, publishEntity, wmstore.BuildWatermarkStore(hb, ot), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	return publishers
}

// buildMessagesForReduce builds test isb.Message which can be used for testing reduce.
func buildMessagesForReduce(count int, key string, publishTime time.Time) []isb.Message {
	var messages = make([]isb.Message, count)
	for i := 0; i < count; i++ {
		result, _ := json.Marshal(PayloadForTest{
			Key:   key,
			Value: i,
		})
		messages[i] = isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: publishTime,
				},
				ID:  fmt.Sprintf("%d", i),
				Key: key,
			},
			Body: isb.Body{Payload: result},
		}
	}

	return messages
}

// writeMessages writes message to simple buffer and publishes the watermark using write offsets
func writeMessages(ctx context.Context, count int, key string, fromBuffer *simplebuffer.InMemoryBuffer, publish publish.Publisher, interval time.Duration) {
	intervalDuration := interval
	publishTime := time.Unix(60, 0)
	i := 1

	for k := 0; k < 3; k++ {
		// build  messages with eventTime set to publish time
		publishTime = publishTime.Add(intervalDuration)
		messages := buildMessagesForReduce(count, key+strconv.Itoa(i), publishTime)
		i++

		// write the messages to fromBuffer, so that it will be available for consuming
		offsets, _ := fromBuffer.Write(ctx, messages)
		for _, offset := range offsets {
			publish.PublishWatermark(wmb.Watermark(publishTime), offset)
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
						publish.PublishWatermark(wmb.Watermark(time.UnixMilli(int64(et))), offsets[len(offsets)-1])
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
			MessageInfo: isb.MessageInfo{
				EventTime: eventTime,
			},
			ID:  fmt.Sprintf("%d", messageValue),
			Key: messageKey,
		},
		Body: isb.Body{Payload: result},
	}
}

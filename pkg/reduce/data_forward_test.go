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
	"go.uber.org/atomic"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned/memory"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/numaproj/numaflow/pkg/window/strategy/session"
)

const pipelineName = "testPipeline"

var keyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: pipelineName,
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
		PipelineName: pipelineName,
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
	lastOffset isb.Offset
	m          sync.Mutex
}

func (e *EventTypeWMProgressor) PublishWatermark(watermark wmb.Watermark, offset isb.Offset, _ int32) {
	e.m.Lock()
	defer e.m.Unlock()
	e.watermarks[offset.String()] = watermark
}

func (e *EventTypeWMProgressor) PublishIdleWatermark(wmb.Watermark, isb.Offset, int32) {
	// TODO
}

func (e *EventTypeWMProgressor) GetLatestWatermark() wmb.Watermark {
	return wmb.Watermark{}
}

func (e *EventTypeWMProgressor) Close() error {
	return nil
}

func (e *EventTypeWMProgressor) ComputeWatermark(offset isb.Offset, _ int32) wmb.Watermark {
	e.lastOffset = offset
	return e.getWatermark()
}

func (e *EventTypeWMProgressor) getWatermark() wmb.Watermark {
	e.m.Lock()
	defer e.m.Unlock()
	return e.watermarks[e.lastOffset.String()]
}

func (e *EventTypeWMProgressor) ComputeHeadWatermark(int32) wmb.Watermark {
	return wmb.Watermark{}
}

func (e *EventTypeWMProgressor) ComputeHeadIdleWMB(int32) wmb.WMB {
	return wmb.WMB{}
}

// PayloadForTest is a dummy payload for testing.
type PayloadForTest struct {
	Key   string
	Value int
}

type myForwardTestRoundRobin struct {
	count atomic.Int32
}

func (f *myForwardTestRoundRobin) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	var output = []forwarder.VertexBuffer{{
		ToVertexName:         "reduce-to-vertex",
		ToVertexPartitionIdx: f.count.Load() % 2,
	}}
	f.count.Add(1)
	return output, nil
}

type CounterReduceTest struct {
}

func (f CounterReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, requestStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse, 1)
	)
	count := 0
	for range requestStream {
		count += 1
	}

	payload := PayloadForTest{Key: "count", Value: count}
	b, _ := json.Marshal(payload)
	ret := isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{
				EventTime: partitionID.End.Add(-1 * time.Millisecond),
			},
			ID:   "msgID",
			Keys: []string{"result"},
		},
		Body: isb.Body{Payload: b},
	}
	response := &window.TimedWindowResponse{
		WriteMessage: &isb.WriteMessage{
			Message: ret,
			Tags:    nil,
		},
		Window: window.NewAlignedTimedWindow(partitionID.Start, partitionID.End, partitionID.Slot),
	}
	responseCh <- response
	return responseCh, errCh
}

func (f CounterReduceTest) WaitUntilReady(_ context.Context) error {
	return nil
}

func (f CounterReduceTest) CloseConn(_ context.Context) error {
	return nil
}

func (f CounterReduceTest) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "reduce-to-vertex",
		ToVertexPartitionIdx: 0,
	}}, nil
}

type SessionSumReduceTest struct {
}

func (s SessionSumReduceTest) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse, 10)
	)
	go func() {
		for msg := range messageStream {
			if msg.Operation == window.Close {
				for _, win := range msg.Windows {
					sum := 0
					if win.Keys()[0] == "even" {
						sum = 6000
					} else {
						sum = 5940
					}
					payload := PayloadForTest{Key: win.Keys()[0], Value: sum}
					b, _ := json.Marshal(payload)
					outputMsg := &isb.WriteMessage{
						Message: isb.Message{
							Header: isb.Header{
								MessageInfo: isb.MessageInfo{
									EventTime: partitionID.End.Add(-1 * time.Millisecond),
								},
								ID:   "msgID",
								Keys: []string{win.Keys()[0]},
							},
							Body: isb.Body{Payload: b},
						},
						Tags: nil,
					}
					response := &window.TimedWindowResponse{
						WriteMessage: outputMsg,
						Window:       window.NewAlignedTimedWindow(partitionID.Start, partitionID.End, partitionID.Slot),
					}
					responseCh <- response
				}
			}
		}
	}()

	return responseCh, errCh
}

func (s SessionSumReduceTest) WaitUntilReady(ctx context.Context) error {
	return nil
}

func (s SessionSumReduceTest) CloseConn(ctx context.Context) error {
	return nil
}

func (s SessionSumReduceTest) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "reduce-to-vertex",
		ToVertexPartitionIdx: 0,
	}}, nil
}

type SumReduceTest struct {
}

func (s SumReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, requestsCh <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse, 2)
	)

	sums := make(map[string]int)

	for msg := range requestsCh {
		var payload PayloadForTest
		_ = json.Unmarshal(msg.ReadMessage.Payload, &payload)
		key := msg.ReadMessage.Keys
		sums[key[0]] += payload.Value
	}

	for k, s := range sums {
		payload := PayloadForTest{Key: k, Value: s}
		b, _ := json.Marshal(payload)
		msg := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: partitionID.End.Add(-1 * time.Millisecond),
					},
					ID:   "msgID",
					Keys: []string{k},
				},
				Body: isb.Body{Payload: b},
			},
			Tags: nil,
		}
		response := &window.TimedWindowResponse{
			WriteMessage: msg,
			Window:       window.NewAlignedTimedWindow(partitionID.Start, partitionID.End, partitionID.Slot),
		}
		responseCh <- response
	}

	return responseCh, errCh
}

func (s SumReduceTest) WaitUntilReady(_ context.Context) error {
	return nil
}

func (s SumReduceTest) CloseConn(_ context.Context) error {
	return nil
}

type MaxReduceTest struct {
}

func (m MaxReduceTest) ApplyReduce(_ context.Context, partitionID *partition.ID, requestCh <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {

	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse, 2)
	)

	mx := math.MinInt64
	maxMap := make(map[string]int)
	for req := range requestCh {
		var payload PayloadForTest
		_ = json.Unmarshal(req.ReadMessage.Payload, &payload)
		if max, ok := maxMap[req.ReadMessage.Keys[0]]; ok {
			mx = max
		}
		if payload.Value > mx {
			mx = payload.Value
			maxMap[req.ReadMessage.Keys[0]] = mx
		}
	}

	for k, max := range maxMap {
		payload := PayloadForTest{Key: k, Value: max}
		b, _ := json.Marshal(payload)
		ret := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: partitionID.End.Add(-1 * time.Millisecond),
					},
					ID:   "msgID",
					Keys: []string{k},
				},
				Body: isb.Body{Payload: b},
			},
			Tags: nil,
		}

		response := &window.TimedWindowResponse{
			WriteMessage: ret,
			Window:       window.NewAlignedTimedWindow(partitionID.Start, partitionID.End, partitionID.Slot),
		}
		responseCh <- response
	}

	return responseCh, errCh
}

func (m MaxReduceTest) WaitUntilReady(_ context.Context) error {
	return nil
}

func (m MaxReduceTest) CloseConn(_ context.Context) error {
	return nil
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
		toVertexName    = "reduce-to-vertex"
	)
	defer cancelFn()
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)
	to := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)

	wmpublisher := &EventTypeWMProgressor{
		watermarks: make(map[string]wmb.Watermark),
	}

	// keep on writing <count> messages every 1 second for the supplied key
	go writeMessages(child, 10, "no-op-test", fromBuffer, wmpublisher, time.Second*1)

	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {to},
	}

	var err error
	var pbqManager *pbq.Manager
	var storeManager store.Manager

	// create store manager
	storeManager = memory.NewMemoryStores(memory.WithStoreSize(100))
	// create pbqManager
	pbqManager, err = pbq.NewManager(child, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	publisher := map[string]publish.Publisher{
		toVertexName: wmpublisher,
	}

	// close publishers
	defer func() {
		for _, p := range publisher {
			_ = p.Close()
		}
	}()

	// create new fixed windower of (windowTime)
	windower := fixed.NewWindower(windowTime, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(child, keyedVertex, CounterReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publisher, idleManager, windower)

	var reduceDataForwarder *DataForward
	reduceDataForwarder, err = NewDataForward(child, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, wmpublisher, publisher,
		windower, idleManager, op, WithReadBatchSize(10))
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
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-child.Done():
			assert.Fail(t, child.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = to.Read(child, 1)
			assert.Nil(t, readErr)
		}
	}

	// assert the output of reduce (count of messages)
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// here the count will always be equal to the number of messages written per key
	assert.Equal(t, 10, readMessagePayload.Value)
	assert.Equal(t, "count", readMessagePayload.Key)

}

// ReadMessage size = 0
func TestReduceDataForward_IdleWM(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 15*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{7}
		startTime      = 1679961600000 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)
	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	toBuffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffers := map[string][]isb.BufferWriter{
		toVertexName: {toBuffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	// source publishes a ctrlMessage and idle watermark so we can do fetch headWMB
	ctrlMessage := []isb.Message{{Header: isb.Header{Kind: isb.WMB}}}
	offsets, errs := fromBuffer.Write(ctx, ctrlMessage)
	assert.Equal(t, make([]error, 1), errs)
	p.PublishIdleWatermark(wmb.Watermark(time.UnixMilli(int64(startTime))), offsets[0], 0)

	publisherMap, otStores := buildPublisherMapAndOTStore(ctx, toBuffers)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publisherMap {
			_ = p.Close()
		}
	}()

	// create a fixed windower of 5s
	windower := fixed.NewWindower(5*time.Second, keyedVertex)
	idleManager := wmb.NewIdleManager(len(toBuffers))
	op := pnf.NewPnFManager(ctx, keyedVertex, CounterReduceTest{}, toBuffers, pbqManager, CounterReduceTest{}, publisherMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffers, pbqManager, storeManager, CounterReduceTest{}, f, publisherMap,
		windower, idleManager, op, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// toBuffers should get a control message after three batch reads
	// 1st read: got one ctrl message, acked and returned
	// 2nd read: len(read)==0 and windowToClose=nil, start the 2 round validation
	// 3rd read: len(read)==0 and windowToClose=nil, validation passes, publish a ctrl msg and idle watermark
	for toBuffer.IsEmpty() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected the toBuffer not to be empty", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	otKeys, _ := otStores[toVertexName].GetAllKeys(ctx)
	for len(otKeys) == 0 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have otKeys", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys, _ = otStores[toVertexName].GetAllKeys(ctx)
		}
	}
	otValue, _ := otStores[toVertexName].GetValue(ctx, otKeys[0])
	otDecode, _ := wmb.DecodeToWMB(otValue)
	for otDecode.Watermark != int64(startTime) { // the first ctrl message written to isb, will use the headWMB to populate wm
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have idle watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otValue, _ = otStores[toVertexName].GetValue(ctx, otKeys[0])
			otDecode, _ = wmb.DecodeToWMB(otValue)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      true,
		Offset:    0, // the first ctrl message written to isb
		Watermark: 1679961600000,
	}, otDecode)

	// check the fromBufferPartition to see if we've acked the message
	for !fromBuffer.IsEmpty() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have acked the ctrl message", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otValue, _ = otStores[toVertexName].GetValue(ctx, otKeys[0])
			otDecode, _ = wmb.DecodeToWMB(otValue)
		}
	}

	// now check the toBuffer to see if we've published the ctrl msg
	assert.Equal(t, isb.WMB, toBuffer.GetMessages(1)[0].Kind)

	// TEST: len(read)==0 and windowToClose exists => close the window
	// publish one data message to start a new window
	publishMessages(ctx, startTime+1000, messageValue, 1, 1, p, fromBuffer)
	// publish a new idle wm to that is smaller than the windowToClose.End()
	offsets, errs = fromBuffer.Write(ctx, ctrlMessage)
	assert.Equal(t, make([]error, 1), errs)
	p.PublishIdleWatermark(wmb.Watermark(time.UnixMilli(int64(startTime+2000))), offsets[0], 0)

	otKeys, _ = otStores[toVertexName].GetAllKeys(ctx)
	otValue, _ = otStores[toVertexName].GetValue(ctx, otKeys[0])
	otDecode, _ = wmb.DecodeToWMB(otValue)
	// wm should be updated using the same offset because it's still idling
	// -1 ms because in this use case we publish watermark-1
	for otDecode.Watermark != int64(startTime+2000-1) {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have idle watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys, _ = otStores[toVertexName].GetAllKeys(ctx)
			otValue, _ = otStores[toVertexName].GetValue(ctx, otKeys[0])
			otDecode, _ = wmb.DecodeToWMB(otValue)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      true,
		Offset:    0, // only update the wm because it's still idling
		Watermark: 1679961601999,
	}, otDecode)
	msgs := toBuffer.GetMessages(10)
	// we didn't read/ack from the toBuffer and also didn't publish any new msgs
	assert.Equal(t, isb.WMB, msgs[0].Kind)
	for i := 1; i < len(msgs); i++ {
		// all the other 9 msgs should be default value
		assert.Equal(t, isb.Header{
			MessageInfo: isb.MessageInfo{},
			Kind:        0,
			ID:          "",
		}, msgs[i].Header)
	}

	// publish a new idle wm to that is larger than the windowToClose.End()
	// the len(read) == 0 now, so the window should be closed
	// the active watermark would be int64(startTime+1000), which is older then int64(startTime+2000-1)
	// so the PublishWatermark will be skipped, but the dataMessage should be published
	p.PublishIdleWatermark(wmb.Watermark(time.UnixMilli(int64(startTime+6000))), offsets[0], 0)

	msgs = toBuffer.GetMessages(10)
	// we didn't read/ack from the toBuffer, so the ctrl should still exist
	assert.Equal(t, isb.WMB, msgs[0].Kind)
	// the second message should be the data message from the closed window above
	// in the test ApplyUDF above we've set the final message to have key="result"
	for len(msgs[1].Keys) == 0 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have data message in buffer", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			msgs = toBuffer.GetMessages(10)
		}
	}
	assert.Equal(t, isb.Data, msgs[1].Kind)
	// in the test ApplyUDF above we've set the final message to have ID="msgID"
	assert.Equal(t, "msgID", msgs[1].ID)
	// in the test ApplyUDF above we've set the final message to have eventTime = partitionID.End-1ms
	assert.Equal(t, int64(1679961604999), msgs[1].EventTime.UnixMilli())
	var result PayloadForTest
	err = json.Unmarshal(msgs[1].Body.Payload, &result)
	assert.NoError(t, err)
	assert.Equal(t, "count", result.Key)
	// because we are using count udf here, and we only send one data message
	// so the value should be 1
	assert.Equal(t, 1, result.Value)
	for i := 2; i < len(msgs); i++ {
		// all the other 8 msgs should be default value
		assert.Equal(t, isb.Header{
			MessageInfo: isb.MessageInfo{},
			Kind:        0,
			ID:          "",
		}, msgs[i].Header)
	}

}

// Count operation with 1 min window
func TestReduceDataForward_Count(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{7}
		startTime      = 60000 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publisherMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publisherMap {
			_ = p.Close()
		}
	}()

	// create a fixed window of 60s
	windower := fixed.NewWindower(60*time.Second, keyedVertex)
	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, CounterReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publisherMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publisherMap,
		windower, idleManager, op, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 300, 10, p, fromBuffer)

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// since the window duration is 60s and tps is 1, the count should be 60
	assert.Equal(t, int64(60), int64(readMessagePayload.Value))
	assert.Equal(t, "count", readMessagePayload.Key)
}

func TestReduceDataForward_AllowedLatencyCount(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		batchSize      = 1
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = 7
		startTime      = 60000 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publisherMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publisherMap {
			_ = p.Close()
		}
	}()

	// create a fixed windower of 10s
	windower := fixed.NewWindower(5*time.Second, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, CounterReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publisherMap, idleManager, windower)

	var reduceDataForward *DataForward
	allowedLatency := 1000
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publisherMap,
		windower, idleManager, op, WithReadBatchSize(int64(batchSize)),
		WithAllowedLateness(time.Duration(allowedLatency)*time.Millisecond),
	)
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessagesAllowedLatency(ctx, startTime, messageValue, allowedLatency, 5, batchSize, p, fromBuffer)

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// without allowedLatency the value would be 4
	assert.Equal(t, int64(5), int64(readMessagePayload.Value))
	assert.Equal(t, "count", readMessagePayload.Key)
}

// Sum operation with 2 minutes window
func TestReduceDataForward_Sum(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{10}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed window of 2 minutes
	windower := fixed.NewWindower(2*time.Minute, keyedVertex)
	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SumReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 300, 10, p, fromBuffer)

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

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
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messageValue   = []int{100}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed window of 5 minutes
	windower := fixed.NewWindower(5*time.Minute, keyedVertex)
	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, MaxReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messageValue, 600, 10, p, fromBuffer)

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	// assert the output of reduce
	var readMessagePayload PayloadForTest
	_ = json.Unmarshal(msgs[0].Payload, &readMessagePayload)
	// since all the messages have the same value the max should be 100
	assert.Equal(t, int64(100), int64(readMessagePayload.Value))
	assert.Equal(t, "even", readMessagePayload.Key)

}

// Sum operation with 5 minutes fixed window and two keys
func TestReduceDataForward_FixedSumWithDifferentKeys(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed windower of 5 minutes
	windower := fixed.NewWindower(5*time.Minute, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SumReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))

	assert.NoError(t, err)

	// start the producer
	go publishMessages(ctx, startTime, messages, 650, 10, p, fromBuffer)

	// start the forwarder
	go reduceDataForward.Start()

	msgs0, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs0) == 0 || msgs0[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs0, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	msgs1, readErr1 := buffer.Read(ctx, 1)
	assert.Nil(t, readErr1)
	for len(msgs1) == 0 || msgs1[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs1, readErr1 = buffer.Read(ctx, 1)
			assert.Nil(t, readErr1)
		}
	}

	// assert the output of reduce
	var readMessagePayload0 PayloadForTest
	_ = json.Unmarshal(msgs0[0].Payload, &readMessagePayload0)
	// since the window duration is 5 minutes, the output should be
	// 100 * 300(for key even) and 99 * 300(for key odd)
	// we cant guarantee the order of the output
	assert.Contains(t, []int{30000, 29700}, readMessagePayload0.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload0.Key)

	var readMessagePayload1 PayloadForTest
	_ = json.Unmarshal(msgs1[0].Payload, &readMessagePayload1)
	assert.Contains(t, []int{30000, 29700}, readMessagePayload1.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload1.Key)

}

// Sum operation with 60 seconds session window and two keys
func TestReduceDataForward_SumWithDifferentKeys(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a session windower with 1 minute timeout
	windower := session.NewWindower(1*time.Minute, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SessionSumReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, SessionSumReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))

	assert.NoError(t, err)

	// start the producer, publishes messages in different sessions
	go publishMessagesWithSession(ctx, startTime, messages, 150, 10, p, fromBuffer)

	// start the forwarder
	go reduceDataForward.Start()

	msgs0, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs0) == 0 || msgs0[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs0, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	msgs1, readErr1 := buffer.Read(ctx, 1)
	assert.Nil(t, readErr1)
	for len(msgs1) == 0 || msgs1[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs1, readErr1 = buffer.Read(ctx, 1)
			assert.Nil(t, readErr1)
		}
	}

	// assert the output of reduce
	var readMessagePayload0 PayloadForTest
	_ = json.Unmarshal(msgs0[0].Payload, &readMessagePayload0)
	// since the session window timeout is 1 minute, the output should be
	// 60 * 100(for key even) and 60 * 99(for key odd)
	assert.Contains(t, []int{6000, 5940}, readMessagePayload0.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload0.Key)

	var readMessagePayload1 PayloadForTest
	_ = json.Unmarshal(msgs1[0].Payload, &readMessagePayload1)
	assert.Contains(t, []int{6000, 5940}, readMessagePayload1.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload1.Key)
}

// Max operation with 5 minutes window and non keyed
func TestReduceDataForward_NonKeyed(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed window of 5 minutes
	windower := fixed.NewWindower(5*time.Minute, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SumReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, nonKeyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()

	// start the producer
	go publishMessages(ctx, startTime, messages, 600, 10, p, fromBuffer)

	// we are reading only one message here but the count should be equal to
	// the number of keyed windows that closed
	msgs, readErr := buffer.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs) == 0 || msgs[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs, readErr = buffer.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

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
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	cctx, childCancel := context.WithCancel(ctx)

	defer cancel()
	defer childCancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer := simplebuffer.NewInMemoryBuffer(toVertexName, toBufferSize, 0)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(100))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(cctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(cctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(cctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed windower of 5 minutes
	windower := fixed.NewWindower(5*time.Minute, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SumReduceTest{}, toBuffer, pbqManager, CounterReduceTest{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(cctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, CounterReduceTest{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(1))
	assert.NoError(t, err)

	// start the forwarder
	go reduceDataForward.Start()
	// window duration is 300s, we are sending only 200 messages with event time less than window end time, so the window will not be closed
	go publishMessages(cctx, startTime, messages, 200, 10, p, fromBuffer)
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

	var discoveredStores []store.Store
	for {
		discoveredStores, _ = storeManager.DiscoverStores(ctx)

		if len(discoveredStores) == 1 {
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
	assert.Len(t, discoveredStores, 1)

}

// Max operation with 5 minutes window and two keys and writing to two partitions
func TestReduceDataForward_SumMultiPartitions(t *testing.T) {
	var (
		ctx, cancel    = context.WithTimeout(context.Background(), 10*time.Second)
		fromBufferSize = int64(100000)
		toBufferSize   = int64(10)
		messages       = []int{100, 99}
		startTime      = 0 // time in millis
		fromBufferName = "source-reduce-buffer"
		toVertexName   = "reduce-to-vertex"
		err            error
	)

	defer cancel()

	// create from buffers
	fromBuffer := simplebuffer.NewInMemoryBuffer(fromBufferName, fromBufferSize, 0)

	// create to buffers
	buffer1 := simplebuffer.NewInMemoryBuffer(toVertexName+"0", toBufferSize, 0)
	buffer2 := simplebuffer.NewInMemoryBuffer(toVertexName+"1", toBufferSize, 1)
	toBuffer := map[string][]isb.BufferWriter{
		toVertexName: {buffer1, buffer2},
	}

	// create store manager
	storeManager := memory.NewMemoryStores(memory.WithStoreSize(1000))

	// create pbq manager
	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, "reduce", pipelineName, 0, storeManager,
		window.Aligned, pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create in memory watermark publisher and fetcher
	f, p := fetcherAndPublisher(ctx, fromBuffer, t.Name())
	publishersMap, _ := buildPublisherMapAndOTStore(ctx, toBuffer)

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishersMap {
			_ = p.Close()
		}
	}()

	// create a fixed windower of 5 minutes
	windower := fixed.NewWindower(5*time.Minute, keyedVertex)

	idleManager := wmb.NewIdleManager(len(toBuffer))
	op := pnf.NewPnFManager(ctx, keyedVertex, SumReduceTest{}, toBuffer, pbqManager, &myForwardTestRoundRobin{}, publishersMap, idleManager, windower)

	var reduceDataForward *DataForward
	reduceDataForward, err = NewDataForward(ctx, keyedVertex, fromBuffer, toBuffer, pbqManager, storeManager, &myForwardTestRoundRobin{}, f, publishersMap,
		windower, idleManager, op, WithReadBatchSize(10))

	assert.NoError(t, err)

	// start the producer
	go publishMessages(ctx, startTime, messages, 650, 10, p, fromBuffer)

	// start the forwarder
	go reduceDataForward.Start()

	msgs0, readErr := buffer1.Read(ctx, 1)
	assert.Nil(t, readErr)
	for len(msgs0) == 0 || msgs0[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs0, readErr = buffer1.Read(ctx, 1)
			assert.Nil(t, readErr)
		}
	}

	msgs1, readErr1 := buffer2.Read(ctx, 1)
	assert.Nil(t, readErr1)
	for len(msgs1) == 0 || msgs1[0].Header.Kind == isb.WMB {
		select {
		case <-ctx.Done():
			assert.Fail(t, ctx.Err().Error())
			return
		default:
			time.Sleep(100 * time.Millisecond)
			msgs1, readErr1 = buffer2.Read(ctx, 1)
			assert.Nil(t, readErr1)
		}
	}

	// assert the output of reduce
	var readMessagePayload0 PayloadForTest
	_ = json.Unmarshal(msgs0[0].Payload, &readMessagePayload0)
	// since the window duration is 5 minutes, the output should be
	// 100 * 300(for key even) and 99 * 300(for key odd)
	// we cant guarantee the order of the output
	assert.Contains(t, []int{30000, 29700}, readMessagePayload0.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload0.Key)

	var readMessagePayload1 PayloadForTest
	_ = json.Unmarshal(msgs1[0].Payload, &readMessagePayload1)
	assert.Contains(t, []int{30000, 29700}, readMessagePayload1.Value)
	assert.Contains(t, []string{"even", "odd"}, readMessagePayload1.Key)

}

// fetcherAndPublisher creates watermark fetcher and publishers, and keeps the processors alive by sending heartbeats
func fetcherAndPublisher(ctx context.Context, fromBuffer *simplebuffer.InMemoryBuffer, key string) (fetch.Fetcher, publish.Publisher) {

	var (
		keyspace = key
	)

	sourcePublishEntity := entity.NewProcessorEntity(fromBuffer.GetName())
	store, _ := wmstore.BuildInmemWatermarkStore(ctx, keyspace)

	heartBeatsCount := atomic.Int32{}
	// publisher for source
	sourcePublisher := publish.NewPublish(ctx, sourcePublishEntity, store, 1, publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))

	// publish heartbeat manually for the processor
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = store.HeartbeatStore().PutKV(ctx, fromBuffer.GetName(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
				time.Sleep(time.Duration(100) * time.Millisecond)
				heartBeatsCount.Inc()
			}
		}
	}()

	edgeFetcherSet := fetch.NewEdgeFetcherSet(ctx, &dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				AbstractVertex: dfv1.AbstractVertex{
					UDF: &dfv1.UDF{
						GroupBy: &dfv1.GroupBy{},
					},
				},
			},
		},
	}, map[string]wmstore.WatermarkStore{"fromVertex": store}, fetch.WithIsReduce(true))

	// wait for 10 heartbeats to be published so that the processor gets created
	for heartBeatsCount.Load() < 10 {
		select {
		case <-ctx.Done():
			return edgeFetcherSet, sourcePublisher
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	return edgeFetcherSet, sourcePublisher
}

func buildPublisherMapAndOTStore(ctx context.Context, toBuffers map[string][]isb.BufferWriter) (map[string]publish.Publisher, map[string]kvs.KVStorer) {
	publishers := make(map[string]publish.Publisher)
	otStores := make(map[string]kvs.KVStorer)

	// create publisher for to Buffers
	index := int32(0)
	for key, partitionedBuffers := range toBuffers {
		publishEntity := entity.NewProcessorEntity(key)
		store, _ := wmstore.BuildInmemWatermarkStore(ctx, key)
		hbWatcherCh, _ := store.HeartbeatStore().Watch(ctx)
		otWatcherCh, _ := store.OffsetTimelineStore().Watch(ctx)
		otStores[key] = store.OffsetTimelineStore()
		p := publish.NewPublish(ctx, publishEntity, store, int32(len(partitionedBuffers)), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-hbWatcherCh:
					// do nothing... just to consume the toBuffer hbBucket
				case <-otWatcherCh:
					// do nothing... just to consume the toBuffer otBucket
				}
			}
		}()
		index++
	}
	return publishers, otStores
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
				ID:   fmt.Sprintf("%d", i),
				Keys: []string{key},
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

		// write the messages to fromBufferPartition, so that it will be available for consuming
		offsets, _ := fromBuffer.Write(ctx, messages)
		for _, offset := range offsets {
			publish.PublishWatermark(wmb.Watermark(publishTime), offset, 0)
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
						publish.PublishWatermark(wmb.Watermark(time.UnixMilli(int64(et))), offsets[len(offsets)-1], 0)
					}
					count = 0
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func publishMessagesWithSession(ctx context.Context, startTime int, messages []int, testDuration int, batchSize int, publish publish.Publisher, fromBuffer *simplebuffer.InMemoryBuffer) {
	eventTime := startTime

	inputChan := make(chan int)

	go func() {
		count := 0
		for i := 0; i < testDuration; i++ {
			if count == 60 {
				eventTime = eventTime + 70*1000
			} else {
				eventTime = eventTime + 1000
			}
			inputChan <- eventTime
			count += 1
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
						publish.PublishWatermark(wmb.Watermark(time.UnixMilli(int64(et))), offsets[len(offsets)-1], 0)
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
	var messageKey []string
	if messageValue%2 == 0 {
		messageKey = []string{"even"}
	} else {
		messageKey = []string{"odd"}
	}

	result, _ := json.Marshal(PayloadForTest{
		Key:   messageKey[0],
		Value: messageValue,
	})
	return isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{
				EventTime: eventTime,
			},
			ID:   fmt.Sprintf("%d", messageValue),
			Keys: messageKey,
		},
		Body: isb.Body{Payload: result},
	}
}

// publishMessagesAllowedLatency is only used for xxxAllowedLatency test
func publishMessagesAllowedLatency(ctx context.Context, startTime int, message int, allowedLatency int, windowSize int, batchSize int, publish publish.Publisher, fromBuffer *simplebuffer.InMemoryBuffer) {
	counter := 0
	inputMsgs := make([]isb.Message, batchSize)
	for i := 0; i <= windowSize; i++ {
		// to simulate real usage
		time.Sleep(time.Second)
		et := startTime + i*1000
		// normal cases, eventTime = watermark
		wm := wmb.Watermark(time.UnixMilli(int64(et)))
		inputMsg := buildIsbMessage(message, time.UnixMilli(int64(et)))
		if i == windowSize-1 {
			// et is now (window.EndTime - 1000)
			et = et + 1000
			// set wm to be the window.EndTime
			// because we've set allowedLatency, so COB is not triggered
			wm = wmb.Watermark(time.UnixMilli(int64(et)))
			// set the eventTime = wm
			inputMsg = buildIsbMessage(message, time.UnixMilli(int64(et)))
		}
		if i == windowSize {
			// et is now window.EndTime
			// set wm to be the window.EndTime + allowedLatency to trigger COB
			wm = wmb.Watermark(time.UnixMilli(int64(et + allowedLatency)))
			// set message eventTime to still be inside the window
			// so the eventTime is now smaller than wm, the message isLate is set to true
			et = et - 100
			inputMsg = buildIsbMessageAllowedLatency(message, time.UnixMilli(int64(et)))
		}
		inputMsgs[counter] = inputMsg
		counter += 1

		if counter >= batchSize {
			offsets, _ := fromBuffer.Write(ctx, inputMsgs)
			if len(offsets) > 0 {
				publish.PublishWatermark(wm, offsets[len(offsets)-1], 0)
			}
			counter = 0
		}
	}

	// dummy messages to
	//  - start a new window
	//  - make sure data forward can
	//    read a message,
	//    get the `6000000` watermark,
	//    and close the first window.
	// COB won't happen for this window
	// will exit early when the test is done
	for i := 0; i <= 10; i++ {
		// to simulate real usage
		time.Sleep(time.Second)
		et := 6000000 + i
		wm := wmb.Watermark(time.UnixMilli(int64(et)))
		inputMsgs = []isb.Message{buildIsbMessage(message, time.UnixMilli(int64(et)))}
		offsets, _ := fromBuffer.Write(ctx, inputMsgs)
		if len(offsets) > 0 {
			publish.PublishWatermark(wm, offsets[len(offsets)-1], 0)
		}
	}
}

func buildIsbMessageAllowedLatency(messageValue int, eventTime time.Time) isb.Message {
	var messageKey []string
	if messageValue%2 == 0 {
		messageKey = []string{"even"}
	} else {
		messageKey = []string{"odd"}
	}

	result, _ := json.Marshal(PayloadForTest{
		Key:   messageKey[0],
		Value: messageValue,
	})
	return isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{
				EventTime: eventTime,
				IsLate:    true,
			},
			ID:   fmt.Sprintf("%d", messageValue),
			Keys: messageKey,
		},
		Body: isb.Body{Payload: result},
	}
}

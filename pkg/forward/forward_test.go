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

package forward

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfapplier "github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publisherHBKeyspace = testPipelineName + "_" + testProcessorEntity + "_%s_" + "PROCESSORS"
	publisherOTKeyspace = testPipelineName + "_" + testProcessorEntity + "_%s_" + "OT"
)

var (
	testStartTime       = time.Unix(1636470000, 0).UTC()
	testSourceWatermark = time.Unix(1636460000, 0).UTC()
	testWMBWatermark    = time.Unix(1636470000, 0).UTC()
)

type testForwardFetcher struct {
	// for forward_test.go only
}

func (t *testForwardFetcher) Close() error {
	// won't be used
	return nil
}

// GetWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testForwardFetcher) GetWatermark(_ isb.Offset) wmb.Watermark {
	return wmb.Watermark(testSourceWatermark)
}

func (t *testForwardFetcher) GetHeadWatermark() wmb.Watermark {
	// won't be used
	return wmb.Watermark{}
}

func (t *testForwardFetcher) GetHeadWMB() wmb.WMB {
	// won't be used
	return wmb.WMB{}
}

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func TestNewInterStepDataForward(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(5))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	count := int64(5)
	// read some data
	readMessages, err := to1.Read(ctx, count)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 5)
	assert.Equal(t, []interface{}{writeMessages[0].Header, writeMessages[1].Header}, []interface{}{readMessages[0].Header, readMessages[1].Header})
	assert.Equal(t, []interface{}{writeMessages[0].Body, writeMessages[1].Body}, []interface{}{readMessages[0].Body, readMessages[1].Body})

	validateMetrics(t)
	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	f.Stop()
	time.Sleep(1 * time.Millisecond)
	// only for shutdown will work as from buffer is not empty
	f.ForceStop()

	<-stopped
}

type testWMBFetcher struct {
	// for forward_test.go only
	WMBTestSameHeadWMB bool // for testing same head wmb, if set true then WMBTestDiffHeadWMB must be false
	sameCounter        int
	sameLock           sync.RWMutex
	WMBTestDiffHeadWMB bool // for testing different head wmb, if set true then WMBTestSameHeadWMB must be false
	diffCounter        int
	diffLock           sync.RWMutex
}

// RevertBoolValue set WMBTestSameHeadWMB and WMBTestDiffHeadWMB to opposite value
func (t *testWMBFetcher) RevertBoolValue() {
	t.sameLock.Lock()
	defer t.sameLock.Unlock()
	t.diffLock.Lock()
	defer t.diffLock.Unlock()
	t.WMBTestSameHeadWMB = !t.WMBTestSameHeadWMB
	t.WMBTestDiffHeadWMB = !t.WMBTestDiffHeadWMB
}

func (t *testWMBFetcher) Close() error {
	// won't be used
	return nil
}

// GetWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testWMBFetcher) GetWatermark(_ isb.Offset) wmb.Watermark {
	return wmb.Watermark(testWMBWatermark)
}

func (t *testWMBFetcher) GetHeadWatermark() wmb.Watermark {
	// won't be used
	return wmb.Watermark{}
}

func (t *testWMBFetcher) GetHeadWMB() wmb.WMB {
	t.sameLock.RLock()
	defer t.sameLock.RUnlock()
	t.diffLock.RLock()
	defer t.diffLock.RUnlock()
	if t.WMBTestSameHeadWMB {
		t.sameCounter++
		if t.sameCounter == 1 || t.sameCounter == 2 {
			return wmb.WMB{
				Idle:      true,
				Offset:    100,
				Watermark: 1636440000000,
			}
		}
		if t.sameCounter == 3 || t.sameCounter == 4 {
			return wmb.WMB{
				Idle:      true,
				Offset:    102,
				Watermark: 1636480000000,
			}
		}
	}
	if t.WMBTestDiffHeadWMB {
		t.diffCounter++
		if t.diffCounter == 1 {
			return wmb.WMB{
				Idle:      true,
				Offset:    100,
				Watermark: 1636440000000,
			}
		}
		if t.diffCounter == 2 {
			return wmb.WMB{
				Idle:      false,
				Offset:    101,
				Watermark: 1636450000000,
			}
		}
	}
	// won't be used
	return wmb.WMB{}
}

func TestNewInterStepDataForwardIdleWatermark(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, simplebuffer.WithReadTimeOut(time.Second)) // default read timeout is 1s
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	ctrlMessage := []isb.Message{{Header: isb.Header{Kind: isb.WMB}}}
	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	fetchWatermark := &testWMBFetcher{WMBTestSameHeadWMB: true}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// first batch: read message size is 1 with one ctrl message
	// the ctrl message should be acked
	// no message published to the next vertex
	// so the timeline should be empty
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for fromStep.IsEmpty() {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					err = ctx.Err()
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()
	_, errs := fromStep.Write(ctx, ctrlMessage)
	assert.Equal(t, make([]error, 1), errs)
	wg.Wait()
	if err != nil {
		t.Fatal("expected the buffer not to be empty", err)
	}
	for !fromStep.IsEmpty() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected the buffer to be empty", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	otNil, _ := otStores["to1"].GetAllKeys(ctx)
	assert.Nil(t, otNil)

	// 2nd and 3rd batches: read message size is 0
	// should send idle watermark
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()

	otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
	otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ := wmb.DecodeToWMB(otValue1)
	for otDecode1.Offset != 0 { // the first ctrl message written to isb. can't use idle because default idle=false
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have idle watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
			otDecode1, _ = wmb.DecodeToWMB(otValue1)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      true,
		Offset:    0, // the first ctrl message written to isb
		Watermark: 1636440000000,
	}, otDecode1)
	assert.Equal(t, isb.WMB, to1.GetMessages(1)[0].Kind)

	// 4th batch: read message size = 1
	// a new active watermark should be inserted
	_, errs = fromStep.Write(ctx, writeMessages[:2])
	assert.Equal(t, make([]error, 2), errs)

	otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
	otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ = wmb.DecodeToWMB(otValue1)
	for otDecode1.Idle {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have active watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
			otDecode1, _ = wmb.DecodeToWMB(otValue1)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    2, // the second message written to isb, read batch size is 2 so the offset is 0+2=2
		Watermark: testWMBWatermark.UnixMilli(),
	}, otDecode1)

	// 5th & 6th batch: again idling but got diff head WMB
	// so the head is still the same active watermark
	// and no new ctrl message to the next vertex
	f.fetchWatermark.(*testWMBFetcher).RevertBoolValue()
	time.Sleep(2 * time.Second) // default read timeout is 1s
	otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
	otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ = wmb.DecodeToWMB(otValue1)
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    2, // the second message written to isb, read batch size is 2 so the offset is 0+2=2
		Watermark: testWMBWatermark.UnixMilli(),
	}, otDecode1)

	var wantKind = []isb.MessageKind{
		isb.WMB,
		isb.Data,
		isb.Data,
		isb.Data, // this is data because it's the default value
	}
	var wantBody = []bool{
		false,
		true,
		true,
		false,
	}
	for idx, msg := range to1.GetMessages(4) {
		assert.Equal(t, wantKind[idx], msg.Kind)
		assert.Equal(t, wantBody[idx], len(msg.Body.Payload) > 0)
	}

	// stop will cancel the contexts and therefore the forwarder stops without waiting
	f.Stop()

	<-stopped
}

func TestNewInterStepDataForwardIdleWatermark_Reset(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, simplebuffer.WithReadTimeOut(time.Second)) // default read timeout is 1s
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	fetchWatermark := &testWMBFetcher{WMBTestSameHeadWMB: true}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	var wg sync.WaitGroup

	// 1st and 2nd batches: read message size is 0
	// should send idle watermark
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()

	otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
	otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ := wmb.DecodeToWMB(otValue1)
	for otDecode1.Offset != 0 { // the first ctrl message written to isb. can't use idle because default idle=false
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have idle watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
			otDecode1, _ = wmb.DecodeToWMB(otValue1)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      true,
		Offset:    0, // the first ctrl message written to isb
		Watermark: 1636440000000,
	}, otDecode1)
	assert.Equal(t, isb.WMB, to1.GetMessages(1)[0].Kind)

	// 3rd batch: read message size = 1
	// a new active watermark should be inserted
	_, errs := fromStep.Write(ctx, writeMessages[:2])
	assert.Equal(t, make([]error, 2), errs)

	otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
	otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ = wmb.DecodeToWMB(otValue1)
	for otDecode1.Idle {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have active watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
			otDecode1, _ = wmb.DecodeToWMB(otValue1)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    2, // the second message written to isb, read batch size is 2 so the offset is 0+2=2
		Watermark: testWMBWatermark.UnixMilli(),
	}, otDecode1)

	// 5th & 6th batch: again idling should send a new ctrl message
	otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
	otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ = wmb.DecodeToWMB(otValue1)
	for otDecode1.Offset != 3 { // the second ctrl message written to isb. can't use idle because default idle=false
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("expected to have idle watermark in to1 timeline", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			otValue1, _ = otStores["to1"].GetValue(ctx, otKeys1[0])
			otDecode1, _ = wmb.DecodeToWMB(otValue1)
		}
	}

	var wantKind = []isb.MessageKind{
		isb.WMB,
		isb.Data,
		isb.Data,
		isb.WMB,
	}
	var wantBody = []bool{
		false,
		true,
		true,
		false,
	}
	for idx, msg := range to1.GetMessages(4) {
		assert.Equal(t, wantKind[idx], msg.Kind)
		assert.Equal(t, wantBody[idx], len(msg.Body.Payload) > 0)
	}

	// stop will cancel the contexts and therefore the forwarder stops without waiting
	f.Stop()

	<-stopped
}

// mySourceForwardTest tests source data transformer by updating message event time, and then verifying new event time and IsLate assignments.
type mySourceForwardTest struct {
}

func (f mySourceForwardTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

// for source forward test, in transformer, we assign to the message a new event time that is before test source watermark,
// such that we can verify message IsLate attribute gets set to true.
var testSourceNewEventTime = testSourceWatermark.Add(time.Duration(-1) * time.Minute)

func (f mySourceForwardTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return func(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
		_ = ctx
		offset := readMessage.ReadOffset
		payload := readMessage.Body.Payload
		parentPaneInfo := readMessage.MessageInfo

		// apply source data transformer
		_ = payload
		// copy the payload
		result := payload
		// assign new event time
		parentPaneInfo.EventTime = testSourceNewEventTime
		var key string

		writeMessage := &isb.Message{
			Header: isb.Header{
				MessageInfo: parentPaneInfo,
				ID:          offset.String(),
				Key:         key,
			},
			Body: isb.Body{
				Payload: result,
			},
		}
		return []*isb.Message{writeMessage}, nil
	}(ctx, message)
}

// TestSourceWatermarkPublisher is a dummy implementation of isb.SourceWatermarkPublisher interface
type TestSourceWatermarkPublisher struct {
}

func (p TestSourceWatermarkPublisher) PublishSourceWatermarks([]*isb.ReadMessage) {
	// PublishSourceWatermarks is not tested in forwarder_test.go
}

func TestSourceInterStepDataForward(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10, simplebuffer.WithReadTimeOut(time.Second*10))
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
	fetchWatermark := &testForwardFetcher{}
	_, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

	// verify if source watermark publisher is not set, NewInterStepDataForward throws.
	failedForwarder, err := NewInterStepDataForward(vertex, fromStep, toSteps, mySourceForwardTest{}, actionsOnFull, mySourceForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(5), WithVertexType(dfv1.VertexTypeSource))
	assert.True(t, failedForwarder == nil)
	assert.Error(t, err)
	assert.Equal(t, fmt.Errorf("failed to assign a non-nil source watermark publisher for source vertex data forwarder"), err)

	// create a valid source forwarder
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, mySourceForwardTest{}, actionsOnFull, mySourceForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(5), WithVertexType(dfv1.VertexTypeSource), WithSourceWatermarkPublisher(TestSourceWatermarkPublisher{}))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	count := int64(2)
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:count])
	assert.Equal(t, make([]error, count), errs)

	// read some data
	readMessages, err := to1.Read(ctx, count)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, int(count))
	assert.Equal(t, []interface{}{writeMessages[0].Header.Key, writeMessages[1].Header.Key}, []interface{}{readMessages[0].Header.Key, readMessages[1].Header.Key})
	assert.Equal(t, []interface{}{writeMessages[0].Header.ID, writeMessages[1].Header.ID}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
	for _, m := range readMessages {
		// verify new event time gets assigned to messages.
		assert.Equal(t, testSourceNewEventTime, m.EventTime)
		// verify messages are marked as late because of event time update.
		assert.Equal(t, true, m.IsLate)
	}
	f.Stop()
	time.Sleep(1 * time.Millisecond)
	// only for shutdown will work as from buffer is not empty
	f.ForceStop()
	<-stopped
}

// TestWriteToBufferError_ActionOnFullIsRetryUntilSuccess explicitly tests the case of retrying failed messages
func TestWriteToBufferError_ActionOnFullIsRetryUntilSuccess(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(10))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()

	go func() {
		for !to1.IsFull() {
			select {
			case <-ctx.Done():
				logging.FromContext(ctx).Fatalf("not full, %s", ctx.Err())
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		// stop will cancel the contexts
		f.Stop()
	}()

	// try to write to buffer after it is full. This causes write to error and fail.
	var messageToStep = make(map[string][]isb.Message)
	messageToStep["to1"] = make([]isb.Message, 0)
	messageToStep["to1"] = append(messageToStep["to1"], writeMessages[0:11]...)

	// asserting the number of failed messages
	_, err = f.writeToBuffers(ctx, messageToStep)
	assert.True(t, strings.Contains(err.Error(), "with failed messages:1"))

	<-stopped
}

// TestWriteToBufferError_ActionOnFullIsDropAndAckLatest explicitly tests the case of dropping messages when buffer is full
func TestWriteToBufferError_ActionOnFullIsDropAndAckLatest(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.DropAndAckLatest,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(10))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
	var messageToStep = make(map[string][]isb.Message)
	messageToStep["to1"] = make([]isb.Message, 0)
	messageToStep["to1"] = append(messageToStep["to1"], writeMessages[0:11]...)
	_, err = f.writeToBuffers(ctx, messageToStep)
	// although we are writing 11 messages to a buffer of size 10, since we specify actionOnFull as DropAndAckLatest,
	// the writeToBuffers() call should return no error.
	assert.Nil(t, err)
	// stop will cancel the contexts and therefore the forwarder stops without waiting
	f.Stop()
	<-stopped
}

// TestNewInterStepDataForwardToOneStep explicitly tests the case where we forward to only one buffer
func TestNewInterStepDataForwardToOneStep(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
		"to2": dfv1.RetryUntilSuccess,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark := &testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, actionsOnFull, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// read some data
	readMessages, err := to1.Read(ctx, 2)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 2)
	assert.Equal(t, []interface{}{writeMessages[0].Header, writeMessages[1].Header}, []interface{}{readMessages[0].Header, readMessages[1].Header})
	assert.Equal(t, []interface{}{writeMessages[0].Body, writeMessages[1].Body}, []interface{}{readMessages[0].Body, readMessages[1].Body})

	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
	otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ := wmb.DecodeToWMB(otValue1)
	assert.False(t, otDecode1.Idle)

	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
		for otKeys2 == nil {
			otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
	otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
	otDecode2, _ := wmb.DecodeToWMB(otValue2)
	assert.True(t, otDecode2.Idle)

	// stop will cancel the contexts and therefore the forwarder stops without waiting
	f.Stop()

	<-stopped
}

type myForwardDropTest struct {
}

func (f myForwardDropTest) WhereTo(_ string) ([]string, error) {
	return []string{dfv1.MessageKeyDrop}, nil
}

func (f myForwardDropTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

// TestNewInterStepDataForwardToOneStep explicitly tests the case where we drop all events
func TestNewInterStepDataForward_dropAll(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
		"to2": dfv1.RetryUntilSuccess,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark := &testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardDropTest{}, actionsOnFull, myForwardDropTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()

	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// nothing to read some data, this is a dropping queue
	assert.Equal(t, true, to1.IsEmpty())

	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
	otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ := wmb.DecodeToWMB(otValue1)
	assert.True(t, otDecode1.Idle)

	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
		for otKeys2 == nil {
			otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
	otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
	otDecode2, _ := wmb.DecodeToWMB(otValue2)
	assert.True(t, otDecode2.Idle)

	// since this is a dropping WhereTo, the buffer can never be full
	f.Stop()

	<-stopped
}

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ string) ([]string, error) {
	return []string{dfv1.MessageKeyAll}, nil
}

func (f myForwardToAllTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

// TestNewInterStepDataForwardToOneStep explicitly tests the case where we forward to all buffers
func TestNewInterStepData_forwardToAll(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
		"to2": dfv1.RetryUntilSuccess,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	fetchWatermark := &testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardToAllTest{}, actionsOnFull, myForwardToAllTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// read some data
	readMessages, err := to1.Read(ctx, 2)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 2)
	assert.Equal(t, []interface{}{writeMessages[0].Header, writeMessages[1].Header}, []interface{}{readMessages[0].Header, readMessages[1].Header})
	assert.Equal(t, []interface{}{writeMessages[0].Body, writeMessages[1].Body}, []interface{}{readMessages[0].Body, readMessages[1].Body})

	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
	otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
	otDecode1, _ := wmb.DecodeToWMB(otValue1)
	assert.False(t, otDecode1.Idle)

	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
		for otKeys2 == nil {
			otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 100)
		}
	}()
	wg.Wait()
	// NOTE: in this test we only have one processor to publish
	// so len(otKeys) should always be 1
	otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
	otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
	otDecode2, _ := wmb.DecodeToWMB(otValue2)
	assert.False(t, otDecode2.Idle)

	f.Stop()

	<-stopped
}

type myForwardInternalErrTest struct {
}

func (f myForwardInternalErrTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardInternalErrTest) ApplyMap(_ context.Context, _ *isb.ReadMessage) ([]*isb.Message, error) {
	return nil, udfapplier.ApplyUDFErr{
		UserUDFErr: false,
		InternalErr: struct {
			Flag        bool
			MainCarDown bool
		}{Flag: true, MainCarDown: false},
		Message: "InternalErr test",
	}
}

func TestNewInterStepDataForward_WithInternalError(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardInternalErrTest{}, actionsOnFull, myForwardInternalErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	f.Stop()
	time.Sleep(1 * time.Millisecond)
	<-stopped
}

type myForwardApplyWhereToErrTest struct {
}

func (f myForwardApplyWhereToErrTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, fmt.Errorf("whereToStep failed")
}

func (f myForwardApplyWhereToErrTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

// TestNewInterStepDataForward_WhereToError is used to test the scenario with error
func TestNewInterStepDataForward_WhereToError(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardApplyWhereToErrTest{}, actionsOnFull, myForwardApplyWhereToErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
	assert.NoError(t, err)
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	f.Stop()
	time.Sleep(1 * time.Millisecond)

	assert.True(t, to1.IsEmpty())
	<-stopped
}

type myForwardApplyUDFErrTest struct {
}

func (f myForwardApplyUDFErrTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardApplyUDFErrTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return nil, fmt.Errorf("UDF error")
}

// TestNewInterStepDataForward_UDFError is used to test the scenario with UDF error
func TestNewInterStepDataForward_UDFError(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	actionsOnFull := map[string]string{
		"to1": dfv1.RetryUntilSuccess,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardApplyUDFErrTest{}, actionsOnFull, myForwardApplyUDFErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
	assert.NoError(t, err)
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	assert.True(t, to1.IsEmpty())

	f.Stop()
	time.Sleep(1 * time.Millisecond)

	<-stopped
}

func validateMetrics(t *testing.T) {
	metadata := `
		# HELP forwarder_read_total Total number of Messages Read
		# TYPE forwarder_read_total counter
		`
	expected := `
		forwarder_read_total{buffer="from",pipeline="testPipeline",vertex="testVertex"} 5
	`

	err := testutil.CollectAndCompare(readMessagesCount, strings.NewReader(metadata+expected), "forwarder_read_total")
	if err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}

	writeMetadata := `
		# HELP forwarder_write_total Total number of Messages Written
		# TYPE forwarder_write_total counter
		`
	writeExpected := `
		forwarder_write_total{buffer="to1",pipeline="testPipeline",vertex="testVertex"} 5
	`

	err = testutil.CollectAndCompare(writeMessagesCount, strings.NewReader(writeMetadata+writeExpected), "forwarder_write_total")
	if err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}

	ackMetadata := `
		# HELP forwarder_ack_total Total number of Messages Acknowledged
		# TYPE forwarder_ack_total counter
		`
	ackExpected := `
		forwarder_ack_total{buffer="from",pipeline="testPipeline",vertex="testVertex"} 5
	`

	err = testutil.CollectAndCompare(ackMessagesCount, strings.NewReader(ackMetadata+ackExpected), "forwarder_ack_total")
	if err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}

}

// buildPublisherMap builds OTStore and publisher for each toBuffer
func buildPublisherMapAndOTStore(toBuffers map[string]isb.BufferWriter) (map[string]publish.Publisher, map[string]wmstore.WatermarkKVStorer) {
	var ctx = context.Background()
	processorEntity := processor.NewProcessorEntity("publisherTestPod")
	publishers := make(map[string]publish.Publisher)
	otStores := make(map[string]wmstore.WatermarkKVStorer)
	for key := range toBuffers {
		heartbeatKV, _, _ := inmem.NewKVInMemKVStore(ctx, testPipelineName, fmt.Sprintf(publisherHBKeyspace, key))
		otKV, _, _ := inmem.NewKVInMemKVStore(ctx, testPipelineName, fmt.Sprintf(publisherOTKeyspace, key))
		otStores[key] = otKV
		p := publish.NewPublish(ctx, processorEntity, wmstore.BuildWatermarkStore(heartbeatKV, otKV), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	return publishers, otStores
}

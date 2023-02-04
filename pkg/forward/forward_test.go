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
	"github.com/numaproj/numaflow/pkg/watermark/ot"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"

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
	testStartTime = time.Unix(1636470000, 0).UTC()
)

type testForwardFetcher struct {
	// for forward_test.go only
}

func (t testForwardFetcher) Close() error {
	// won't be used
	return nil
}

// GetWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t testForwardFetcher) GetWatermark(_ isb.Offset) processor.Watermark {
	return processor.Watermark(time.Now())
}

func (t testForwardFetcher) GetHeadWatermark() processor.Watermark {
	// won't be used
	return processor.Watermark{}
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(5))
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

// TestWriteToBufferError explicitly tests the case of retrying failed messages
func TestWriteToBufferError(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(10))
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

// TestNewInterStepDataForwardToOneStep explicitly tests the case where we forward to only one buffer
func TestNewInterStepDataForwardToOneStep(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
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

	fetchWatermark := testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
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
	otDecode1, _ := ot.DecodeToOTValue(otValue1)
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
	otDecode2, _ := ot.DecodeToOTValue(otValue2)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark := testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardDropTest{}, myForwardDropTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
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
	otDecode1, _ := ot.DecodeToOTValue(otValue1)
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
	otDecode2, _ := ot.DecodeToOTValue(otValue2)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	fetchWatermark := testForwardFetcher{}
	publishWatermark, otStores := buildPublisherMapAndOTStore(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardToAllTest{}, myForwardToAllTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2), WithVertexType(dfv1.VertexTypeMapUDF))
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
	otDecode1, _ := ot.DecodeToOTValue(otValue1)
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
	otDecode2, _ := ot.DecodeToOTValue(otValue2)
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardInternalErrTest{}, myForwardInternalErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardApplyWhereToErrTest{}, myForwardApplyWhereToErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardApplyUDFErrTest{}, myForwardApplyUDFErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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

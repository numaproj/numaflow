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
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/numaproj/numaflow/pkg/shared/logging"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	udfapplier "github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/stretchr/testify/assert"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
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

type myForwardDropTest struct {
}

func (f myForwardDropTest) WhereTo(_ string) ([]string, error) {
	return []string{"__DROP__"}, nil
}

func (f myForwardDropTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func TestNewInterStepDataForward_drop(t *testing.T) {
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardDropTest{}, myForwardDropTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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

	// since this is a dropping WhereTo, the buffer can never be full
	f.Stop()

	<-stopped
}

type myForwardApplyErrTest struct {
}

func (f myForwardApplyErrTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardApplyErrTest) Apply(_ context.Context, _ *isb.ReadMessage) ([]*isb.Message, error) {
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
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardApplyErrTest{}, myForwardApplyErrTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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

func (f myForwardApplyWhereToErrTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
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

func (f myForwardApplyUDFErrTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {

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

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ string) ([]string, error) {
	return []string{dfv1.MessageKeyAll}, nil
}

func (f myForwardToAllTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

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
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardToAllTest{}, myForwardToAllTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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

	f.Stop()

	<-stopped
}

// TestNewInterStepDataForwardToOneStep explicitly tests the case where we forward to only one step
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

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
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

	// stop will cancel the contexts and therefore the forwarder stops with out waiting
	f.Stop()

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

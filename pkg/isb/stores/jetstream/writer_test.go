//go:build isb_jetstream

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

package jetstream

import (
	"context"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
)

type myForwardJetStreamTest struct {
}

func (f myForwardJetStreamTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardJetStreamTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

// TestForwarderJetStreamBuffer is a test that is used to test forwarder with jetstream buffer
func TestForwarderJetStreamBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestForwarderJetStreamBuffer"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	toStreamName := "TestForwarderJetStreamBuffer-to"
	addStream(t, js, toStreamName)
	defer deleteStream(js, toStreamName)

	bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, WithMaxLength(10))
	assert.NoError(t, err)
	jw, _ := bw.(*jetStreamWriter)
	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(10), startTime)
	// Verify if buffer is not full.
	for jw.isFull.Load() {
		select {
		case <-ctx.Done():
			t.Fatalf("expected not to be full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	// Add some data to buffer using write
	_, errs := jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 10)

	// Verify if buffer is full.
	for !jw.isFull.Load() {
		select {
		case <-ctx.Done():
			t.Fatalf("full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	// Forwarder logic tested here with a jetstream read and write
	bufferReader, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)

	fromStep, _ := bufferReader.(*jetStreamReader)

	bufferWriter, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, toStreamName, toStreamName, toStreamName, WithMaxLength(10))
	assert.NoError(t, err)

	to1 := bufferWriter.(*jetStreamWriter)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := forward.NewInterStepDataForward(vertex, fromStep, toSteps, myForwardJetStreamTest{}, myForwardJetStreamTest{}, fetchWatermark, publishWatermark)
	assert.NoError(t, err)

	stopped := f.Start()

	to1js, err := to1.conn.JetStream()
	assert.NoError(t, err)
	streamInfo, err := to1js.StreamInfo(toStreamName)
	assert.NoError(t, err)

	// Verify if number of messages in toBuffer is 10 (which is maxlength and hence buffer is full).
	for streamInfo.State.Msgs != 10 {
		streamInfo, _ = to1js.StreamInfo(toStreamName)
		if streamInfo.State.Msgs == 10 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("expected 10 msgs, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// assert toBuffer is full and all messages appear in toBuffer
	time.Sleep(2 * time.Second) // wait for isFull check.
	assert.True(t, to1.isFull.Load())

	fromStepJs, err := fromStep.conn.JetStream()
	assert.NoError(t, err)
	fromStepInfo, err := fromStepJs.StreamInfo(streamName)
	assert.NoError(t, err)
	// Make sure all messages are cleared up from from buffer as DiscardOldPolicy is false
	assert.Equal(t, uint64(0), fromStepInfo.State.Msgs)

	// Call stop to end the test as we have a blocking read. The forwarder is up and running with no messages written
	f.Stop()

	<-stopped
}

// TestJetStreamBufferWrite on buffer full
func TestJetStreamBufferWriterBufferFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestJetStreamBufferWriterBufferFull"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, WithMaxLength(10), WithBufferUsageLimit(0.2))
	assert.NoError(t, err)
	jw, _ := bw.(*jetStreamWriter)
	timeout := time.After(10 * time.Second)
	for jw.isFull.Load() {
		select {
		case <-timeout:
			t.Fatalf("expected not to be full")
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(2), startTime)
	// Add some data to buffer using write and verify no writes are performed when buffer is full
	_, errs := jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Nil(t, errMsg) // Buffer not full, expect no error
	}
	timeout = time.After(10 * time.Second)
	for !jw.isFull.Load() {
		select {
		case <-timeout:
			t.Fatalf("expected to be full")
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
	messages = testutils.BuildTestWriteMessages(int64(2), time.Unix(1636470001, 0))
	_, errs = jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Error(t, errMsg)
		assert.Contains(t, errMsg.Error(), "full") // Expect buffer full error
	}
}

// TestGetName is used to test the GetName function
func TestWriteGetName(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestWriteGetName"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bufferWriter, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)

	bw := bufferWriter.(*jetStreamReader)
	assert.Equal(t, bw.GetName(), streamName)

}

// TestWriteClose is used to test Close function in Write
func TestWriteClose(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestWriteClose"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bufferWriter, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)

	bw := bufferWriter.(*jetStreamWriter)
	assert.NoError(t, bw.Close())

}

// TestConvert2NatsMsgHeader is used to convert nats header
func TestConvert2NatsMsgHeader(t *testing.T) {
	isbHeader := isb.Header{
		PaneInfo: isb.PaneInfo{
			EventTime: time.Unix(1636470000, 0),
			StartTime: time.Unix(1636470000, 0),
			EndTime:   time.Unix(1636470060, 0),
		},
		ID: "1",
	}

	assert.NotNil(t, convert2NatsMsgHeader(isbHeader))

}

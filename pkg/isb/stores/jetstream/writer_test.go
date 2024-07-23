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

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	natsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"

	"github.com/numaproj/numaflow/pkg/udf/forward"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type myForwardJetStreamTest struct {
}

func (f myForwardJetStreamTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardJetStreamTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, "test-vertex", message)
}

func (f myForwardJetStreamTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, "", writeMessageCh, message)
}

func (f myForwardJetStreamTest) ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	return testutils.CopyUDFTestApplyBatchMap(ctx, "test-vertex", messages)
}

// TestForwarderJetStreamBuffer is a test that is used to test forwarder with jetstream buffer
func TestForwarderJetStreamBuffer(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int64
		streamEnabled bool
		unaryEnabled  bool
		batchEnabled  bool
	}{
		{
			name:          "unary",
			batchSize:     10,
			streamEnabled: false,
			unaryEnabled:  true,
			batchEnabled:  false,
		},
		{
			name:          "stream",
			batchSize:     1,
			streamEnabled: true,
			unaryEnabled:  false,
			batchEnabled:  false,
		},
		{
			name:          "batch_map",
			batchSize:     10,
			streamEnabled: false,
			unaryEnabled:  false,
			batchEnabled:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := natstest.RunJetStreamServer(t)
			defer natstest.ShutdownJetStreamServer(t, s)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			defaultJetStreamClient := natsclient.NewTestClientWithServer(t, s)
			defer defaultJetStreamClient.Close()
			js, err := defaultJetStreamClient.JetStreamContext()
			assert.NoError(t, err)

			streamName := "TestForwarderJetStreamBuffer"
			addStream(t, js, streamName)
			defer deleteStream(t, js, streamName)

			toStreamName := "TestForwarderJetStreamBuffer-to"
			addStream(t, js, toStreamName)
			defer deleteStream(t, js, toStreamName)

			bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx, WithMaxLength(10))
			assert.NoError(t, err)
			jw, _ := bw.(*jetStreamWriter)
			defer jw.Close()
			// Add some data
			startTime := time.Unix(1636470000, 0)
			messages := testutils.BuildTestWriteMessages(int64(10), startTime, nil, "testVertex")
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

			vertexInstance := &dfv1.VertexInstance{
				Vertex:  vertex,
				Replica: 0,
			}

			// Forwarder logic tested here with a jetstream read and write
			bufferReader, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx)
			assert.NoError(t, err)
			fromStep, _ := bufferReader.(*jetStreamReader)
			defer fromStep.Close()

			bufferWriter, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, toStreamName, toStreamName, toStreamName, defaultPartitionIdx, WithMaxLength(10))
			assert.NoError(t, err)
			to1 := bufferWriter.(*jetStreamWriter)
			defer to1.Close()
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager, err := wmb.NewIdleManager(1, len(toSteps))
			assert.NoError(t, err)

			opts := []forward.Option{forward.WithReadBatchSize(tt.batchSize)}
			if tt.batchEnabled {
				opts = append(opts, forward.WithUDFBatchMap(myForwardJetStreamTest{}))
			}
			if tt.streamEnabled {
				opts = append(opts, forward.WithUDFStreamingMap(myForwardJetStreamTest{}))
			}
			if tt.unaryEnabled {
				opts = append(opts, forward.WithUDFUnaryMap(myForwardJetStreamTest{}))
			}

			f, err := forward.NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardJetStreamTest{}, fetchWatermark, publishWatermark, idleManager, opts...)
			assert.NoError(t, err)

			stopped := f.Start()

			to1js, err := to1.client.JetStreamContext()
			assert.NoError(t, err)
			streamInfo, err := to1js.StreamInfo(toStreamName)
			assert.NoError(t, err)

			// Verify if number of messages in toBuffer is 10 (which is maxlength and hence buffer is full).
			for streamInfo.State.Msgs != 10 {
				streamInfo, _ = to1js.StreamInfo(toStreamName)
				if streamInfo.State.Msgs == 10 {
					break
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

			fromStepJs, err := fromStep.client.JetStreamContext()
			assert.NoError(t, err)
			fromStepInfo, err := fromStepJs.StreamInfo(streamName)
			assert.NoError(t, err)
			// Make sure all messages are cleared up from buffer as DiscardOldPolicy is false
			assert.Equal(t, uint64(0), fromStepInfo.State.Msgs)

			// Call stop to end the test as we have a blocking read. The forwarder is up and running with no messages written
			f.Stop()

			<-stopped
		})
	}
}

// TestJetStreamBufferWrite on buffer full
func TestJetStreamBufferWriterBufferFull(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natsclient.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()
	js, err := defaultJetStreamClient.JetStreamContext()
	assert.NoError(t, err)

	streamName := "TestJetStreamBufferWriterBufferFull"
	addStream(t, js, streamName)
	defer deleteStream(t, js, streamName)

	bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx, WithMaxLength(10), WithBufferUsageLimit(0.2))
	assert.NoError(t, err)
	jw, _ := bw.(*jetStreamWriter)
	defer jw.Close()
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
	messages := testutils.BuildTestWriteMessages(int64(2), startTime, nil, "testVertex")
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
	messages = testutils.BuildTestWriteMessages(int64(2), time.Unix(1636470001, 0), nil, "testVertex")
	_, errs = jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Error(t, errMsg)
		assert.Contains(t, errMsg.Error(), "full") // Expect buffer full error
	}
}

// TestJetStreamBufferWrite on buffer full, with writing strategy being DiscardLatest
func TestJetStreamBufferWriterBufferFull_DiscardLatest(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natsclient.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()
	js, err := defaultJetStreamClient.JetStreamContext()
	assert.NoError(t, err)

	streamName := "TestJetStreamBufferWriterBufferFull"
	addStream(t, js, streamName)
	defer deleteStream(t, js, streamName)

	bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx, WithMaxLength(10), WithBufferUsageLimit(0.2), WithBufferFullWritingStrategy(dfv1.DiscardLatest))
	assert.NoError(t, err)
	jw, _ := bw.(*jetStreamWriter)
	defer jw.Close()
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
	messages := testutils.BuildTestWriteMessages(int64(2), startTime, nil, "testVertex")
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
	messages = testutils.BuildTestWriteMessages(int64(2), time.Unix(1636470001, 0), nil, "testVertex")
	_, errs = jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Equal(t, errMsg, isb.NonRetryableBufferWriteErr{Name: streamName, Message: isb.BufferFullMessage})
	}
}

// TestGetName is used to test the GetName function
func TestWriteGetName(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natsclient.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()
	js, err := defaultJetStreamClient.JetStreamContext()
	assert.NoError(t, err)

	streamName := "TestWriteGetName"
	addStream(t, js, streamName)
	defer deleteStream(t, js, streamName)

	bufferWriter, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx)
	assert.NoError(t, err)

	bw := bufferWriter.(*jetStreamReader)
	defer bw.Close()
	assert.Equal(t, bw.GetName(), streamName)

}

// TestWriteClose is used to test Close function in Write
func TestWriteClose(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natsclient.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()
	js, err := defaultJetStreamClient.JetStreamContext()
	assert.NoError(t, err)

	streamName := "TestWriteClose"
	addStream(t, js, streamName)
	defer deleteStream(t, js, streamName)

	bufferWriter, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, defaultPartitionIdx)
	assert.NoError(t, err)

	bw := bufferWriter.(*jetStreamWriter)
	defer bw.Close()
	assert.NoError(t, bw.Close())
}

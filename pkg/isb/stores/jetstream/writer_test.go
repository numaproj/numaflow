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

	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
)

type myForwardJetStreamTest struct {
}

func (f myForwardJetStreamTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardJetStreamTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (f myForwardJetStreamTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
}

// TestForwarderJetStreamPartition is a test that is used to test forwarder with jetstream partition
func TestForwarderJetStreamPartition(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int64
		streamEnabled bool
	}{
		{
			name:          "batch",
			batchSize:     10,
			streamEnabled: false,
		},
		{
			name:          "stream",
			batchSize:     1,
			streamEnabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := natstest.RunJetStreamServer(t)
			defer natstest.ShutdownJetStreamServer(t, s)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			defaultJetStreamClient := natstest.JetStreamClient(t, s)
			conn, err := defaultJetStreamClient.Connect(ctx)
			assert.NoError(t, err)
			defer conn.Close()
			js, err := conn.JetStream()
			assert.NoError(t, err)

			streamName := "TestForwarderJetStreamPartition"
			addStream(t, js, streamName)
			defer deleteStream(js, streamName)

			toStreamName := "TestForwarderJetStreamPartition-to"
			addStream(t, js, toStreamName)
			defer deleteStream(js, toStreamName)

			bw, err := NewJetStreamWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0, WithMaxLength(10))
			assert.NoError(t, err)
			jw, _ := bw.(*jetStreamWriter)
			defer jw.Close()
			// Add some data
			startTime := time.Unix(1636470000, 0)
			messages := testutils.BuildTestWriteMessages(int64(10), startTime)
			// Verify if partition is not full.
			for jw.isFull.Load() {
				select {
				case <-ctx.Done():
					t.Fatalf("expected not to be full, %s", ctx.Err())
				default:
					time.Sleep(1 * time.Millisecond)
				}
			}
			// Add some data to partition using write
			_, errs := jw.Write(ctx, messages)
			assert.Equal(t, len(errs), 10)

			// Verify if partition is full.
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
			partitionReader, err := NewJetStreamReader(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0)
			assert.NoError(t, err)
			fromStep, _ := partitionReader.(*jetStreamReader)
			defer fromStep.Close()

			partitionWriter, err := NewJetStreamWriter(ctx, defaultJetStreamClient, toStreamName, toStreamName, toStreamName, 0, WithMaxLength(10))
			assert.NoError(t, err)
			to1 := partitionWriter.(*jetStreamWriter)
			defer to1.Close()
			toSteps := map[string][]isb.PartitionWriter{
				"to1": {to1},
			}

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromPartitionMap(toSteps)
			f, err := forward.NewInterStepDataForward(vertex, fromStep, toSteps, myForwardJetStreamTest{}, myForwardJetStreamTest{}, fetchWatermark, publishWatermark, forward.WithReadBatchSize(tt.batchSize), forward.WithUDFStreaming(tt.streamEnabled))
			assert.NoError(t, err)

			stopped := f.Start()

			to1js, err := to1.conn.JetStream()
			assert.NoError(t, err)
			streamInfo, err := to1js.StreamInfo(toStreamName)
			assert.NoError(t, err)

			// Verify if number of messages in toPartition is 10 (which is maxlength and hence partition is full).
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

			// assert toPartition is full and all messages appear in toPartition
			time.Sleep(2 * time.Second) // wait for isFull check.
			assert.True(t, to1.isFull.Load())

			fromStepJs, err := fromStep.conn.JetStream()
			assert.NoError(t, err)
			fromStepInfo, err := fromStepJs.StreamInfo(streamName)
			assert.NoError(t, err)
			// Make sure all messages are cleared up from partition as DiscardOldPolicy is false
			assert.Equal(t, uint64(0), fromStepInfo.State.Msgs)

			// Call stop to end the test as we have a blocking read. The forwarder is up and running with no messages written
			f.Stop()

			<-stopped
		})
	}
}

// TestJetStreamPartitionWrite on partition full
func TestJetStreamPartitionWriterPartitionFull(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natstest.JetStreamClient(t, s)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestJetStreamPartitionWriterPartitionFull"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bw, err := NewJetStreamWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0, WithMaxLength(10), WithPartitionUsageLimit(0.2))
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
	messages := testutils.BuildTestWriteMessages(int64(2), startTime)
	// Add some data to partition using write and verify no writes are performed when partition is full
	_, errs := jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Nil(t, errMsg) // partition not full, expect no error
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
		assert.Contains(t, errMsg.Error(), "full") // Expect partition full error
	}
}

// TestJetStreamPartitionWrite on partition full, with writing strategy being DiscardLatest
func TestJetStreamPartitionWriterBufferFull_DiscardLatest(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natstest.JetStreamClient(t, s)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestJetStreamPartitionWriterBufferFull"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bw, err := NewJetStreamWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0, WithMaxLength(10), WithPartitionUsageLimit(0.2), WithPartitionFullWritingStrategy(dfv1.DiscardLatest))
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
	messages := testutils.BuildTestWriteMessages(int64(2), startTime)
	// Add some data to buffer using write and verify no writes are performed when partition is full
	_, errs := jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 2)
	for _, errMsg := range errs {
		assert.Nil(t, errMsg) // partition not full, expect no error
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
		assert.Equal(t, errMsg, isb.NonRetryablePartitionWriteErr{Name: streamName, Message: "Partition full!"})
	}
}

// TestGetName is used to test the GetName function
func TestWriteGetName(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natstest.JetStreamClient(t, s)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestWriteGetName"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	partitionReader, err := NewJetStreamReader(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0)
	assert.NoError(t, err)

	bw := partitionReader.(*jetStreamReader)
	defer bw.Close()
	assert.Equal(t, bw.GetName(), streamName)

}

// TestWriteClose is used to test Close function in Write
func TestWriteClose(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	defaultJetStreamClient := natstest.JetStreamClient(t, s)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "TestWriteClose"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	partitionWriter, err := NewJetStreamWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName, 0)
	assert.NoError(t, err)

	bw := partitionWriter.(*jetStreamWriter)
	defer bw.Close()
	assert.NoError(t, bw.Close())
}

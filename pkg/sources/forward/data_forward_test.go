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

	"go.uber.org/goleak"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfapplier "github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publishKeyspace     = testPipelineName + "_" + testProcessorEntity + "_%s"
)

var (
	testStartTime       = time.Unix(1636470000, 0).UTC()
	testSourceWatermark = time.Unix(1636460000, 0).UTC()
)

type SimpleSource struct {
	buffer *simplebuffer.InMemoryBuffer
}

func (s *SimpleSource) Pending(ctx context.Context) (int64, error) {
	return 0, nil
}

func NewSimpleSource(buffer *simplebuffer.InMemoryBuffer) *SimpleSource {
	return &SimpleSource{buffer: buffer}
}

func (s *SimpleSource) Close() error {
	return s.buffer.Close()
}

func (s *SimpleSource) GetName() string {
	return s.buffer.GetName()
}

func (s *SimpleSource) Read(ctx context.Context, i int64) ([]*isb.ReadMessage, error) {
	return s.buffer.Read(ctx, i)
}

func (s *SimpleSource) Ack(ctx context.Context, offsets []isb.Offset) []error {
	return s.buffer.Ack(ctx, offsets)
}

func (s *SimpleSource) Partitions(context.Context) []int32 {
	return []int32{0}
}

func (s *SimpleSource) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	return s.buffer.Write(ctx, messages)
}

type testForwardFetcher struct {
	// for data_forward_test.go only
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// ComputeWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testForwardFetcher) ComputeWatermark() wmb.Watermark {
	return t.getWatermark()
}

func (t *testForwardFetcher) getWatermark() wmb.Watermark {
	return wmb.Watermark(testSourceWatermark)
}

func (t *testForwardFetcher) ComputeHeadWatermark(int32) wmb.Watermark {
	return wmb.Watermark{}
}

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardTest) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	out := make([]isb.ReadWriteMessagePair, len(messages))
	for i, msg := range messages {
		writeMsg, _ := testutils.CopyUDFTestApply(ctx, "test-vertex", msg)
		out[i] = isb.ReadWriteMessagePair{
			ReadMessage:   msg,
			WriteMessages: writeMsg,
		}
	}
	return out, nil
}

func TestNewDataForward(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int64
	}{
		{
			name:      "batch_forward",
			batchSize: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+"_basic", func(t *testing.T) {
			metricsReset()
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to11 := simplebuffer.NewInMemoryBuffer("to1-1", 2*batchSize, 0)
			to12 := simplebuffer.NewInMemoryBuffer("to1-2", 2*batchSize, 1)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to11, to12},
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")
			fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toSteps)
			noOpStores := buildNoOpToVertexStores(toSteps)
			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, fetchWatermark, TestSourceWatermarkPublisher{}, noOpStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardTest{}))

			assert.NoError(t, err)
			assert.False(t, to11.IsFull())
			assert.False(t, to12.IsFull())

			assert.True(t, to11.IsEmpty())
			assert.True(t, to12.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)

			var updatedBatchSize int
			if tt.batchSize > 1 {
				updatedBatchSize = int(batchSize / 2)
			} else {
				updatedBatchSize = int(batchSize)
			}
			// read some data
			readMessages, err := to11.Read(ctx, int64(updatedBatchSize))
			assert.NoError(t, err, "expected no error")
			assert.Len(t, readMessages, updatedBatchSize)
			for i, j := 0, 0; i < updatedBatchSize; i, j = i+1, j+2 {
				assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
				assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
				assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
				assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
			}

			if tt.batchSize > 1 {
				readMessages, err = to12.Read(ctx, int64(updatedBatchSize))
				assert.NoError(t, err, "expected no error")
				assert.Len(t, readMessages, updatedBatchSize)
				for i, j := 0, 1; i < updatedBatchSize; i, j = i+1, j+2 {
					assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
					assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
					assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
					assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
				}
			}

			// iterate to see whether metrics will eventually succeed
			err = validateMetrics(batchSize)
			for err != nil {
				select {
				case <-ctx.Done():
					t.Errorf("failed waiting metrics collection to succeed [%s] (%s)", ctx.Err(), err)
				default:
					time.Sleep(10 * time.Millisecond)
					err = validateMetrics(batchSize)
				}
			}

			// write some data
			_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
			assert.Equal(t, make([]error, 3*batchSize), errs)

			f.Stop()
			time.Sleep(1 * time.Millisecond)
			// only for shutdown will work as from buffer is not empty
			f.ForceStop()

			<-stopped

		})
		// Explicitly tests the case where we forward to all buffers
		t.Run(tt.name+"_toAll", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 10*batchSize, 0))
			to11 := simplebuffer.NewInMemoryBuffer("to1-1", 2*batchSize, 0)
			to12 := simplebuffer.NewInMemoryBuffer("to1-2", 2*batchSize, 1)

			to21 := simplebuffer.NewInMemoryBuffer("to2-1", 2*batchSize, 0)
			to22 := simplebuffer.NewInMemoryBuffer("to2-2", 2*batchSize, 1)

			toSteps := map[string][]isb.BufferWriter{
				"to1": {to11, to12},
				"to2": {to21, to22},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

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
			fetchWatermark := &testForwardFetcher{}
			toVertexStores := buildToVertexWatermarkStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, &myForwardToAllTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(&myForwardToAllTest{}))

			assert.NoError(t, err)
			assert.False(t, to11.IsFull())
			assert.False(t, to12.IsFull())

			assert.True(t, to11.IsEmpty())
			assert.True(t, to12.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)

			var updatedBatchSize int
			if tt.batchSize > 1 {
				updatedBatchSize = int(batchSize / 2)
			} else {
				updatedBatchSize = int(batchSize)
			}
			// read some data
			readMessages, err := to11.Read(ctx, int64(updatedBatchSize))
			assert.NoError(t, err, "expected no error")
			assert.Len(t, readMessages, updatedBatchSize)
			for i, j := 0, 0; i < updatedBatchSize; i, j = i+1, j+2 {
				assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
				assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
				assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
				assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
			}

			if tt.batchSize > 1 {
				// read some data
				readMessages, err = to12.Read(ctx, int64(updatedBatchSize))
				assert.NoError(t, err, "expected no error")
				assert.Len(t, readMessages, updatedBatchSize)
				for i, j := 0, 1; i < updatedBatchSize; i, j = i+1, j+2 {
					assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
					assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
					assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
					assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
				}
			}
			// write some data
			_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
			assert.Equal(t, make([]error, 3*batchSize), errs)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys1, _ := toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue1, _ := toVertexStores["to1"].OffsetTimelineStore().GetValue(ctx, otKeys1[0])
							otDecode1, _ := wmb.DecodeToWMB(otValue1)
							if tt.batchSize > 1 && !otDecode1.Idle {
								break loop
							} else if tt.batchSize == 1 && otDecode1.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys2, _ := toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue2, _ := toVertexStores["to2"].OffsetTimelineStore().GetValue(ctx, otKeys2[0])
							otDecode1, _ := wmb.DecodeToWMB(otValue2)
							// if the batch size is 1, then the watermark should be idle because
							// one of partitions will be idle
							if tt.batchSize > 1 && !otDecode1.Idle {
								break loop
							} else if tt.batchSize == 1 && otDecode1.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()
			wg.Wait()
			f.Stop()
			<-stopped
		})
		// Explicitly tests the case where we drop all events
		t.Run(tt.name+"_dropAll", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to11 := simplebuffer.NewInMemoryBuffer("to1-1", 2*batchSize, 0)
			to12 := simplebuffer.NewInMemoryBuffer("to1-2", 2*batchSize, 1)

			to21 := simplebuffer.NewInMemoryBuffer("to2-1", 2*batchSize, 0)
			to22 := simplebuffer.NewInMemoryBuffer("to2-2", 2*batchSize, 1)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to11, to12},
				"to2": {to21, to22},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

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

			fetchWatermark := &testForwardFetcher{}
			toVertexStores := buildToVertexWatermarkStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, myForwardDropTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardDropTest{}))

			assert.NoError(t, err)
			assert.False(t, to11.IsFull())
			assert.False(t, to12.IsFull())

			assert.True(t, to11.IsEmpty())
			assert.True(t, to12.IsEmpty())

			stopped := f.Start()

			// write some data
			_, errs := fromStep.Write(ctx, writeMessages)
			assert.Equal(t, make([]error, 4*batchSize), errs)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys1, _ := toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue1, _ := toVertexStores["to2"].OffsetTimelineStore().GetValue(ctx, otKeys1[0])
							otDecode1, _ := wmb.DecodeToWMB(otValue1)
							// if the batch size is 1, then the watermark should be idle because
							// one of partitions will be idle
							if otDecode1.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys2, _ := toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue2, _ := toVertexStores["to2"].OffsetTimelineStore().GetValue(ctx, otKeys2[0])
							otDecode2, _ := wmb.DecodeToWMB(otValue2)
							// if the batch size is 1, then the watermark should be idle because
							// one of partitions will be idle
							if otDecode2.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()
			wg.Wait()

			msgs := to11.GetMessages(1)
			for len(msgs) == 0 || msgs[0].Kind != isb.WMB {
				select {
				case <-ctx.Done():
					logging.FromContext(ctx).Fatalf("expect to have the ctrl message in to1, %s", ctx.Err())
				default:
					msgs = to11.GetMessages(1)
					time.Sleep(1 * time.Millisecond)
				}
			}

			msgs = to12.GetMessages(1)
			for len(msgs) == 0 || msgs[0].Kind != isb.WMB {
				select {
				case <-ctx.Done():
					logging.FromContext(ctx).Fatalf("expect to have the ctrl message in to1, %s", ctx.Err())
				default:
					msgs = to12.GetMessages(1)
					time.Sleep(1 * time.Millisecond)
				}
			}

			msgs = to21.GetMessages(1)
			for len(msgs) == 0 || msgs[0].Kind != isb.WMB {
				select {
				case <-ctx.Done():
					logging.FromContext(ctx).Fatalf("expect to have the ctrl message in to2, %s", ctx.Err())
				default:
					msgs = to21.GetMessages(1)
					time.Sleep(1 * time.Millisecond)
				}
			}

			msgs = to22.GetMessages(1)
			for len(msgs) == 0 || msgs[0].Kind != isb.WMB {
				select {
				case <-ctx.Done():
					logging.FromContext(ctx).Fatalf("expect to have the ctrl message in to2, %s", ctx.Err())
				default:
					msgs = to22.GetMessages(1)
					time.Sleep(1 * time.Millisecond)
				}
			}

			// since this is a dropping WhereTo, the buffer can never be full
			f.Stop()

			<-stopped
		})
		// Explicitly tests the case where we forward to only one buffer
		t.Run(tt.name+"_toOneStep", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to11 := simplebuffer.NewInMemoryBuffer("to1-1", 2*batchSize, 0)
			to12 := simplebuffer.NewInMemoryBuffer("to1-2", 2*batchSize, 1)
			to21 := simplebuffer.NewInMemoryBuffer("to2-1", 2*batchSize, 0)
			to22 := simplebuffer.NewInMemoryBuffer("to2-2", 2*batchSize, 1)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to11, to12},
				"to2": {to21, to22},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

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

			fetchWatermark := &testForwardFetcher{}
			toVertexStores := buildToVertexWatermarkStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardTest{}))

			assert.NoError(t, err)
			assert.False(t, to11.IsFull())
			assert.False(t, to12.IsFull())

			assert.True(t, to11.IsEmpty())
			assert.True(t, to12.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)
			var updatedBatchSize int
			if tt.batchSize > 1 {
				updatedBatchSize = int(batchSize / 2)
			} else {
				updatedBatchSize = int(batchSize)
			}
			// read some data
			readMessages, err := to11.Read(ctx, int64(updatedBatchSize))
			assert.NoError(t, err, "expected no error")

			assert.Len(t, readMessages, updatedBatchSize)
			for i, j := 0, 0; i < updatedBatchSize; i, j = i+1, j+2 {
				assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
				assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
				assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
				assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
			}
			// write some data
			_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
			assert.Equal(t, make([]error, 3*batchSize), errs)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys1, _ := toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue1, _ := toVertexStores["to1"].OffsetTimelineStore().GetValue(ctx, otKeys1[0])
							otDecode1, _ := wmb.DecodeToWMB(otValue1)
							if tt.batchSize > 1 && !otDecode1.Idle {
								break loop
							} else if tt.batchSize == 1 && otDecode1.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				otKeys2, _ := toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
							otValue2, _ := toVertexStores["to2"].OffsetTimelineStore().GetValue(ctx, otKeys2[0])
							otDecode2, _ := wmb.DecodeToWMB(otValue2)
							// if the batch size is 1, then the watermark should be idle because
							// one of partitions will be idle
							if otDecode2.Idle {
								break loop
							} else {
								time.Sleep(time.Millisecond * 10)
							}
						}
					}

				}
			}()
			wg.Wait()

			// stop will cancel the contexts and therefore the forwarder stops without waiting
			f.Stop()

			<-stopped
		})
		// Test the scenario with Transformer error
		t.Run(tt.name+"_TransformerError", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

			fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toSteps)
			toVertexStores := buildNoOpToVertexStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, myForwardApplyTransformerErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardApplyTransformerErrTest{}))

			assert.NoError(t, err)
			assert.False(t, to1.IsFull())
			assert.True(t, to1.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)
			assert.True(t, to1.IsEmpty())

			f.Stop()
			time.Sleep(1 * time.Millisecond)

			<-stopped
		})
		// Test the scenario with error
		t.Run(tt.name+"_whereToError", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

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

			fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toSteps)
			toVertexStores := buildNoOpToVertexStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, myForwardApplyWhereToErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardApplyWhereToErrTest{}))

			assert.NoError(t, err)
			assert.True(t, to1.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)

			f.Stop()
			time.Sleep(1 * time.Millisecond)

			assert.True(t, to1.IsEmpty())
			<-stopped
		})
		t.Run(tt.name+"_withInternalError", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0))
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil, "testVertex")

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

			fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toSteps)
			toVertexStores := buildNoOpToVertexStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, myForwardInternalErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(batchSize), WithTransformer(myForwardInternalErrTest{}))

			assert.NoError(t, err)
			assert.False(t, to1.IsFull())
			assert.True(t, to1.IsEmpty())

			stopped := f.Start()
			// write some data
			_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
			assert.Equal(t, make([]error, batchSize), errs)

			f.Stop()
			time.Sleep(1 * time.Millisecond)
			<-stopped
		})
	}
}

// mySourceForwardTest tests source data transformer by updating message event time, and then verifying new event time and IsLate assignments.
type mySourceForwardTest struct {
}

func (f mySourceForwardTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

type mySourceForwardTestRoundRobin struct {
	count int
}

func (f *mySourceForwardTestRoundRobin) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	var output = []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: int32(f.count % 2),
	}}
	f.count++
	return output, nil
}

// for source forward test, in transformer, we assign to the message a new event time that is before test source watermark,
// such that we can verify message IsLate attribute gets set to true.
var testSourceNewEventTime = testSourceWatermark.Add(time.Duration(-1) * time.Minute)

func (f mySourceForwardTest) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	results := make([]isb.ReadWriteMessagePair, len(messages))
	for i, message := range messages {
		message.MessageInfo.EventTime = testSourceNewEventTime
		writeMsg := isb.Message{
			Header: isb.Header{
				MessageInfo: message.MessageInfo,
				ID: isb.MessageID{
					VertexName: "test-vertex",
					Offset:     message.ReadOffset.String(),
				},
				Keys: []string{},
			},
			Body: isb.Body{
				Payload: message.Body.Payload,
			},
		}
		results[i] = isb.ReadWriteMessagePair{
			ReadMessage: message,
			WriteMessages: []*isb.WriteMessage{{
				Message: writeMsg,
			}},
		}
	}
	return results, nil
}

// TestSourceWatermarkPublisher is a dummy implementation of isb.SourceWatermarkPublisher interface
type TestSourceWatermarkPublisher struct {
}

func (p TestSourceWatermarkPublisher) PublishIdleWatermarks(time.Time, []int32) {
	// PublishIdleWatermarks is not tested in data_forwarder_test.go
}

func (p TestSourceWatermarkPublisher) PublishSourceWatermarks([]*isb.ReadMessage) {
	// PublishSourceWatermarks is not tested in data_forwarder_test.go
}

func TestDataForwardSinglePartition(t *testing.T) {
	fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 25, 0))
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10, 0, simplebuffer.WithReadTimeOut(time.Second*10))
	toSteps := map[string][]isb.BufferWriter{
		"to1": {to1},
	}
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "receivingVertex",
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil, "testVertex")
	fetchWatermark := &testForwardFetcher{}
	toVertexStores := buildNoOpToVertexStores(toSteps)

	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewDataForward(vertexInstance, fromStep, toSteps, mySourceForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(5), WithTransformer(mySourceForwardTest{}))
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
	assert.Equal(t, []interface{}{writeMessages[0].Header.Keys, writeMessages[1].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
	assert.Equal(t, []interface{}{isb.MessageID{VertexName: "test-vertex", Offset: "0-0", Index: 0}, isb.MessageID{VertexName: "test-vertex", Offset: "1-0", Index: 0}}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
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

func TestDataForwardMultiplePartition(t *testing.T) {
	fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 25, 0))
	to11 := simplebuffer.NewInMemoryBuffer("to1-0", 10, 0, simplebuffer.WithReadTimeOut(time.Second*10))
	to12 := simplebuffer.NewInMemoryBuffer("to1-1", 10, 1, simplebuffer.WithReadTimeOut(time.Second*10))
	toSteps := map[string][]isb.BufferWriter{
		"to1": {to11, to12},
	}
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "receivingVertex",
		},
	}}
	vertexInstance := &dfv1.VertexInstance{
		Vertex:  vertex,
		Replica: 0,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil, "testVertex")
	fetchWatermark := &testForwardFetcher{}
	toVertexStores := buildNoOpToVertexStores(toSteps)

	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(5), WithTransformer(mySourceForwardTest{}))
	assert.NoError(t, err)
	assert.False(t, to11.IsFull())
	assert.False(t, to12.IsFull())
	assert.True(t, to11.IsEmpty())
	assert.True(t, to12.IsEmpty())

	stopped := f.Start()
	count := int64(4)
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:count])
	assert.Equal(t, make([]error, count), errs)

	time.Sleep(time.Second)
	// read some data
	// since we have produced four messages, both the partitions should have two messages each.(we write in round-robin fashion)
	readMessages, err := to11.Read(ctx, 2)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 2)
	assert.Equal(t, []interface{}{writeMessages[0].Header.Keys, writeMessages[2].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
	assert.Equal(t, []interface{}{
		isb.MessageID{
			VertexName: "test-vertex",
			Offset:     "0-0",
			Index:      0,
		},
		isb.MessageID{
			VertexName: "test-vertex",
			Offset:     "2-0",
			Index:      0,
		},
	}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
	for _, m := range readMessages {
		// verify new event time gets assigned to messages.
		assert.Equal(t, testSourceNewEventTime, m.EventTime)
		// verify messages are marked as late because of event time update.
		assert.Equal(t, true, m.IsLate)
	}

	time.Sleep(time.Second)

	readMessages, err = to12.Read(ctx, 2)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 2)
	assert.Equal(t, []interface{}{writeMessages[1].Header.Keys, writeMessages[3].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
	assert.Equal(t, []interface{}{
		isb.MessageID{
			VertexName: "test-vertex",
			Offset:     "1-0",
			Index:      0,
		},
		isb.MessageID{
			VertexName: "test-vertex",
			Offset:     "3-0",
			Index:      0,
		},
	}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
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

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	tests := []struct {
		name       string
		batchSize  int64
		strategy   dfv1.BufferFullWritingStrategy
		throwError bool
	}{
		{
			name:      "test-discard-latest",
			batchSize: 10,
			strategy:  dfv1.DiscardLatest,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:      "test-retry-until-success",
			batchSize: 10,
			strategy:  dfv1.RetryUntilSuccess,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
		{
			name:      "test-discard-latest",
			batchSize: 1,
			strategy:  dfv1.DiscardLatest,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:      "test-retry-until-success",
			batchSize: 1,
			strategy:  dfv1.RetryUntilSuccess,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			fromStep := NewSimpleSource(simplebuffer.NewInMemoryBuffer("from", 5*value.batchSize, 0))
			buffer := simplebuffer.NewInMemoryBuffer("to1", value.batchSize, 0, simplebuffer.WithBufferFullWritingStrategy(value.strategy))
			toSteps := map[string][]isb.BufferWriter{
				"to1": {buffer},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
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

			fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toSteps)
			toVertexStores := buildNoOpToVertexStores(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewDataForward(vertexInstance, fromStep, toSteps, myForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, idleManager, WithReadBatchSize(value.batchSize), WithTransformer(myForwardTest{}))
			assert.NoError(t, err)
			assert.False(t, buffer.IsFull())
			assert.True(t, buffer.IsEmpty())

			stopped := f.Start()
			go func() {
				for !buffer.IsFull() {
					select {
					case <-ctx.Done():
						logging.FromContext(ctx).Fatalf("not full, %s", ctx.Err())
					default:
						time.Sleep(1 * time.Millisecond)
					}
				}
				// stop will cancel the context
				f.Stop()
			}()

			// try to write to buffer after it is full.
			var messageToStep = make(map[string][][]isb.Message)
			messageToStep["to1"] = make([][]isb.Message, 1)
			writeMessages := testutils.BuildTestWriteMessages(4*value.batchSize, testStartTime, nil, "testVertex")
			messageToStep["to1"][0] = append(messageToStep["to1"][0], writeMessages[0:value.batchSize+1]...)
			_, err = f.writeToBuffers(ctx, messageToStep)

			assert.Equal(t, value.throwError, err != nil)
			if value.throwError {
				// assert the number of failed messages
				assert.True(t, strings.Contains(err.Error(), "with failed messages:1"))
			}
			<-stopped
		})
	}
}

type myForwardDropTest struct {
}

func (f myForwardDropTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{}, nil
}

func (f myForwardDropTest) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	results := make([]isb.ReadWriteMessagePair, len(messages))
	for i, message := range messages {
		writeMsg, _ := testutils.CopyUDFTestApply(ctx, "test-vertex", message)
		results[i] = isb.ReadWriteMessagePair{
			ReadMessage:   message,
			WriteMessages: writeMsg,
		}
	}
	return results, nil
}

type myForwardToAllTest struct {
	count int
}

func (f *myForwardToAllTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	var output = []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: int32(f.count % 2),
	},
		{
			ToVertexName:         "to2",
			ToVertexPartitionIdx: int32(f.count % 2),
		}}
	f.count++
	return output, nil
}

func (f *myForwardToAllTest) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	results := make([]isb.ReadWriteMessagePair, len(messages))
	for i, message := range messages {
		writeMsg, _ := testutils.CopyUDFTestApply(ctx, "test-vertex", message)
		results[i] = isb.ReadWriteMessagePair{
			ReadMessage:   message,
			WriteMessages: writeMsg,
		}
	}
	return results, nil
}

type myForwardInternalErrTest struct {
}

func (f myForwardInternalErrTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardInternalErrTest) ApplyTransform(ctx context.Context, _ []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	return nil, &udfapplier.ApplyUDFErr{
		UserUDFErr: false,
		InternalErr: struct {
			Flag        bool
			MainCarDown bool
		}{Flag: true, MainCarDown: false},
		Message: "InternalErr test",
	}
}

type myForwardApplyWhereToErrTest struct {
}

func (f myForwardApplyWhereToErrTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, fmt.Errorf("whereToStep failed")
}

func (f myForwardApplyWhereToErrTest) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	results := make([]isb.ReadWriteMessagePair, len(messages))
	for i, message := range messages {
		writeMsg, _ := testutils.CopyUDFTestApply(ctx, "test-vertex", message)
		results[i] = isb.ReadWriteMessagePair{
			ReadMessage:   message,
			WriteMessages: writeMsg,
		}
	}
	return results, nil
}

type myForwardApplyTransformerErrTest struct {
}

func (f myForwardApplyTransformerErrTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardApplyTransformerErrTest) ApplyTransform(_ context.Context, _ []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	return nil, fmt.Errorf("transformer error")
}

func validateMetrics(batchSize int64) (err error) {
	metadata := `
		# HELP forwarder_read_total Total number of Messages Read
		# TYPE forwarder_read_total counter
		`
	expected := `
		forwarder_read_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
	`

	err = testutil.CollectAndCompare(metrics.ReadMessagesCount, strings.NewReader(metadata+expected), "forwarder_read_total")
	if err != nil {
		return err
	}

	writeMetadata := `
		# HELP forwarder_write_total Total number of Messages Written
		# TYPE forwarder_write_total counter
		`
	var writeExpected string
	if batchSize > 1 {
		writeExpected = `
		forwarder_write_total{partition_name="to1-1",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%f", float64(batchSize)/2) + `
		forwarder_write_total{partition_name="to1-2",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%f", float64(batchSize)/2) + `
	`
	} else {
		writeExpected = `
		forwarder_write_total{partition_name="to1-1",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
		forwarder_write_total{partition_name="to1-2",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%f", float64(0)) + `
	`
	}

	err = testutil.CollectAndCompare(metrics.WriteMessagesCount, strings.NewReader(writeMetadata+writeExpected), "forwarder_write_total")
	if err != nil {
		return err
	}

	ackMetadata := `
		# HELP forwarder_ack_total Total number of Messages Acknowledged
		# TYPE forwarder_ack_total counter
		`
	ackExpected := `
		forwarder_ack_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Source"} ` + fmt.Sprintf("%d", batchSize) + `
	`

	err = testutil.CollectAndCompare(metrics.AckMessagesCount, strings.NewReader(ackMetadata+ackExpected), "forwarder_ack_total")
	if err != nil {
		return err
	}

	return nil
}

func metricsReset() {
	metrics.ReadMessagesCount.Reset()
	metrics.WriteMessagesCount.Reset()
	metrics.AckMessagesCount.Reset()
}

// buildPublisherMap builds OTStore and publisher for each toBuffer
func buildToVertexWatermarkStores(toBuffers map[string][]isb.BufferWriter) map[string]wmstore.WatermarkStore {
	var ctx = context.Background()
	otStores := make(map[string]wmstore.WatermarkStore)
	for key := range toBuffers {
		store, _ := wmstore.BuildInmemWatermarkStore(ctx, fmt.Sprintf(publishKeyspace, key))
		otStores[key] = store
	}
	return otStores
}

func buildNoOpToVertexStores(toBuffers map[string][]isb.BufferWriter) map[string]wmstore.WatermarkStore {
	toVertexStores := make(map[string]wmstore.WatermarkStore)
	for key := range toBuffers {
		store, _ := wmstore.BuildNoOpWatermarkStore()
		toVertexStores[key] = store
	}
	return toVertexStores
}

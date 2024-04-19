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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	udfapplier "github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publisherKeyspace   = testPipelineName + "_" + testProcessorEntity + "_%s"
)

var (
	testStartTime       = time.Unix(1636470000, 0).UTC()
	testSourceWatermark = time.Unix(1636460000, 0).UTC()
	testWMBWatermark    = time.Unix(1636470000, 0).UTC()
)

type testForwardFetcher struct {
	// for forward_test.go only
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func (t *testForwardFetcher) ComputeWatermark(offset isb.Offset, partition int32) wmb.Watermark {
	return t.getWatermark()
}

// getWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testForwardFetcher) getWatermark() wmb.Watermark {
	return wmb.Watermark(testSourceWatermark)
}

func (t *testForwardFetcher) ComputeHeadIdleWMB(int32) wmb.WMB {
	// won't be used
	return wmb.WMB{}
}

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (f myForwardTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
}

func TestNewInterStepDataForward(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int64
		streamEnabled bool
	}{
		{
			name:          "stream_forward",
			batchSize:     1,
			streamEnabled: true,
		},
		{
			name:          "batch_forward",
			batchSize:     10,
			streamEnabled: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+"_basic", func(t *testing.T) {
			metricsReset()
			batchSize := tt.batchSize
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
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

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
			validateMetrics(t, batchSize)
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
			fromStep := simplebuffer.NewInMemoryBuffer("from", 10*batchSize, 0)
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

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

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
			toVertexWmStores := buildWatermarkStores(toSteps)
			publishWatermark, otStores := buildPublisherMapAndOTStoreFromWmStores(toSteps, toVertexWmStores)

			defer func() {
				for _, p := range publishWatermark {
					_ = p.Close()
				}
			}()

			defer func() {
				for _, store := range toVertexWmStores {
					_ = store.Close()
				}
			}()

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, &myForwardToAllTest{}, &myForwardToAllTest{}, &myForwardToAllTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
				otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
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
				otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
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
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
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

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

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
			toVertexWmStores := buildWatermarkStores(toSteps)
			publishWatermark, otStores := buildPublisherMapAndOTStoreFromWmStores(toSteps, toVertexWmStores)

			defer func() {
				for _, p := range publishWatermark {
					_ = p.Close()
				}
			}()

			defer func() {
				for _, store := range toVertexWmStores {
					_ = store.Close()
				}
			}()

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardDropTest{}, myForwardDropTest{}, myForwardDropTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
				otKeys1, _ := otStores["to2"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
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
				otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
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
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
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

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

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
			toVertexWmStores := buildWatermarkStores(toSteps)
			publishWatermark, otStores := buildPublisherMapAndOTStoreFromWmStores(toSteps, toVertexWmStores)

			defer func() {
				for _, p := range publishWatermark {
					_ = p.Close()
				}
			}()

			defer func() {
				for _, store := range toVertexWmStores {
					_ = store.Close()
				}
			}()

			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
				otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys1) == 0 {
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
							otValue1, _ := otStores["to1"].GetValue(ctx, otKeys1[0])
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
				otKeys2, _ := otStores["to2"].GetAllKeys(ctx)
			loop:
				for {
					select {
					case <-ctx.Done():
						assert.Fail(t, "context cancelled while waiting to get a valid watermark")
						break loop
					default:
						if len(otKeys2) == 0 {
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							time.Sleep(time.Millisecond * 10)
						} else {
							// NOTE: in this test we only have one processor to publish
							// so len(otKeys) should always be 1
							otKeys2, _ = otStores["to2"].GetAllKeys(ctx)
							otValue2, _ := otStores["to2"].GetValue(ctx, otKeys2[0])
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
		// Test the scenario with UDF error
		t.Run(tt.name+"_UDFError", func(t *testing.T) {
			batchSize := tt.batchSize
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
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

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardApplyUDFErrTest{}, myForwardApplyUDFErrTest{}, myForwardApplyUDFErrTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

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

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardApplyWhereToErrTest{}, myForwardApplyWhereToErrTest{}, myForwardApplyWhereToErrTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
			to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
			toSteps := map[string][]isb.BufferWriter{
				"to1": {to1},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

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

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardInternalErrTest{}, myForwardInternalErrTest{}, myForwardInternalErrTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(batchSize), WithUDFStreaming(tt.streamEnabled))

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

func (t *testWMBFetcher) ComputeWatermark(offset isb.Offset, partition int32) wmb.Watermark {
	return t.getWatermark()
}

// getWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testWMBFetcher) getWatermark() wmb.Watermark {
	return wmb.Watermark(testWMBWatermark)
}

func (t *testWMBFetcher) ComputeHeadWatermark(int32) wmb.Watermark {
	// won't be used
	return wmb.Watermark{}
}

func (t *testWMBFetcher) ComputeHeadIdleWMB(int32) wmb.WMB {
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
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0, simplebuffer.WithReadTimeOut(time.Second)) // default read timeout is 1s
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10, 0)
	toSteps := map[string][]isb.BufferWriter{
		"to1": {to1},
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

	ctrlMessage := []isb.Message{{Header: isb.Header{Kind: isb.WMB}}}
	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil)

	fetchWatermark := &testWMBFetcher{WMBTestSameHeadWMB: true}
	toVertexWmStores := buildWatermarkStores(toSteps)
	publishWatermark, otStores := buildPublisherMapAndOTStoreFromWmStores(toSteps, toVertexWmStores)

	defer func() {
		for _, p := range publishWatermark {
			_ = p.Close()
		}
	}()

	defer func() {
		for _, store := range toVertexWmStores {
			_ = store.Close()
		}
	}()

	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardTest{}, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(2))
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())
	assert.True(t, to1.IsEmpty())

	stopped := f.Start()
	assert.True(t, fromStep.IsEmpty())
	// first batch: read message size is 1 with one ctrl message
	// the ctrl message should be acked
	// no message published to the next vertex
	// so the timeline should be empty
	_, errs := fromStep.Write(ctx, ctrlMessage)
	assert.Equal(t, make([]error, 1), errs)
	// waiting for the ctrl message to be acked
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
	// it should not publish any wm because
	// 1. readLength != 0
	// 2. we only have one ctrl message
	// 3. meaning dataMessage=0
	otNil, _ := otStores["to1"].GetAllKeys(ctx)
	assert.Nil(t, otNil)

	// 2nd and 3rd batches: read message size is 0
	// should send idle watermark
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		otKeys1, _ := otStores["to1"].GetAllKeys(ctx)
		for otKeys1 == nil {
			otKeys1, _ = otStores["to1"].GetAllKeys(ctx)
			time.Sleep(time.Millisecond * 10)
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
	f.wmFetcher.(*testWMBFetcher).RevertBoolValue()
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
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0, simplebuffer.WithReadTimeOut(time.Second)) // default read timeout is 1s
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10, 0)
	toSteps := map[string][]isb.BufferWriter{
		"to1": {to1},
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

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil)

	fetchWatermark := &testWMBFetcher{WMBTestSameHeadWMB: true}
	toVertexWmStores := buildWatermarkStores(toSteps)
	publishWatermark, otStores := buildPublisherMapAndOTStoreFromWmStores(toSteps, toVertexWmStores)

	defer func() {
		for _, p := range publishWatermark {
			_ = p.Close()
		}
	}()

	defer func() {
		for _, store := range toVertexWmStores {
			_ = store.Close()
		}
	}()

	// close the fetcher and publishers
	defer func() {
		for _, p := range publishWatermark {
			_ = p.Close()
		}
	}()

	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardTest{}, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(2))
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
			time.Sleep(time.Millisecond * 10)
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

func (f mySourceForwardTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return func(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
		_ = ctx
		offset := readMessage.ReadOffset
		payload := readMessage.Body.Payload
		parentPaneInfo := readMessage.MessageInfo

		// apply source data transformer
		_ = payload
		// copy the payload
		result := payload
		var key []string

		writeMessage := isb.Message{
			Header: isb.Header{
				MessageInfo: parentPaneInfo,
				ID:          offset.String(),
				Keys:        key,
			},
			Body: isb.Body{
				Payload: result,
			},
		}
		return []*isb.WriteMessage{{Message: writeMessage}}, nil
	}(ctx, message)
}

func (f mySourceForwardTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return func(ctx context.Context, readMessage *isb.ReadMessage, writeMessages chan<- isb.WriteMessage) error {
		defer close(writeMessages)

		_ = ctx
		offset := readMessage.ReadOffset
		payload := readMessage.Body.Payload
		parentPaneInfo := readMessage.MessageInfo

		// apply source data transformer
		_ = payload
		// copy the payload
		result := payload
		var key []string

		writeMessage := isb.Message{
			Header: isb.Header{
				MessageInfo: parentPaneInfo,
				ID:          offset.String(),
				Keys:        key,
			},
			Body: isb.Body{
				Payload: result,
			},
		}

		writeMessages <- isb.WriteMessage{Message: writeMessage}
		return nil
	}(ctx, message, writeMessageCh)
}

// TestSourceWatermarkPublisher is a dummy implementation of isb.SourcePublisher interface
type TestSourceWatermarkPublisher struct {
}

func (p TestSourceWatermarkPublisher) PublishSourceWatermarks([]*isb.ReadMessage) {
	// PublishSourceWatermarks is not tested in forwarder_test.go
}

func TestInterStepDataForwardSinglePartition(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
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

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil)
	fetchWatermark := &testForwardFetcher{}
	_, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

	// create a forwarder
	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, mySourceForwardTest{}, mySourceForwardTest{}, mySourceForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(5))
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
	assert.Equal(t, []interface{}{"0-0-receivingVertex-0", "1-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
	assert.Equal(t, []interface{}{writeMessages[0].Header.Headers, writeMessages[1].Header.Headers}, []interface{}{readMessages[0].Header.Headers, readMessages[1].Header.Headers})

	f.Stop()
	time.Sleep(1 * time.Millisecond)
	// only for shutdown will work as from buffer is not empty
	f.ForceStop()
	<-stopped
}

func TestInterStepDataForwardMultiplePartition(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
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

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime, nil)
	fetchWatermark := &testForwardFetcher{}
	_, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)

	// create a forwarder

	idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
	f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, mySourceForwardTest{}, mySourceForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(5))
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
	assert.Equal(t, []interface{}{"0-0-receivingVertex-0", "2-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
	assert.Equal(t, []interface{}{writeMessages[0].Header.Headers, writeMessages[2].Header.Headers}, []interface{}{readMessages[0].Header.Headers, readMessages[1].Header.Headers})

	time.Sleep(time.Second)

	readMessages, err = to12.Read(ctx, 2)
	assert.NoError(t, err, "expected no error")
	assert.Len(t, readMessages, 2)
	assert.Equal(t, []interface{}{writeMessages[1].Header.Keys, writeMessages[3].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
	assert.Equal(t, []interface{}{"1-0-receivingVertex-0", "3-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
	assert.Equal(t, []interface{}{writeMessages[1].Header.Headers, writeMessages[3].Header.Headers}, []interface{}{readMessages[0].Header.Headers, readMessages[1].Header.Headers})

	f.Stop()
	time.Sleep(1 * time.Millisecond)
	// only for shutdown will work as from buffer is not empty
	f.ForceStop()
	<-stopped
}

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int64
		strategy      dfv1.BufferFullWritingStrategy
		streamEnabled bool
		throwError    bool
	}{
		{
			name:          "test-discard-latest",
			batchSize:     10,
			strategy:      dfv1.DiscardLatest,
			streamEnabled: false,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:          "test-retry-until-success",
			batchSize:     10,
			strategy:      dfv1.RetryUntilSuccess,
			streamEnabled: false,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
		{
			name:          "test-discard-latest",
			batchSize:     1,
			strategy:      dfv1.DiscardLatest,
			streamEnabled: true,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:          "test-retry-until-success",
			batchSize:     1,
			strategy:      dfv1.RetryUntilSuccess,
			streamEnabled: true,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*value.batchSize, 0)
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

			fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
			idleManager, _ := wmb.NewIdleManager(1, len(toSteps))
			f, err := NewInterStepDataForward(vertexInstance, fromStep, toSteps, myForwardTest{}, myForwardTest{}, myForwardTest{}, fetchWatermark, publishWatermark, idleManager, WithReadBatchSize(value.batchSize), WithUDFStreaming(value.streamEnabled))
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
			writeMessages := testutils.BuildTestWriteMessages(4*value.batchSize, testStartTime, nil)
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

func (f myForwardDropTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (f myForwardDropTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
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

func (f *myForwardToAllTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (f myForwardToAllTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
}

type myForwardInternalErrTest struct {
}

func (f myForwardInternalErrTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardInternalErrTest) ApplyMap(_ context.Context, _ *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return nil, &udfapplier.ApplyUDFErr{
		UserUDFErr: false,
		InternalErr: struct {
			Flag        bool
			MainCarDown bool
		}{Flag: true, MainCarDown: false},
		Message: "InternalErr test",
	}
}

func (f myForwardInternalErrTest) ApplyMapStream(_ context.Context, _ *isb.ReadMessage, writeMessagesCh chan<- isb.WriteMessage) error {
	close(writeMessagesCh)
	return &udfapplier.ApplyUDFErr{
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

func (f myForwardApplyWhereToErrTest) ApplyMap(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (f myForwardApplyWhereToErrTest) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return testutils.CopyUDFTestApplyStream(ctx, message, writeMessageCh)
}

type myForwardApplyUDFErrTest struct {
}

func (f myForwardApplyUDFErrTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func (f myForwardApplyUDFErrTest) ApplyMap(_ context.Context, _ *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return nil, fmt.Errorf("UDF error")
}

func (f myForwardApplyUDFErrTest) ApplyMapStream(_ context.Context, _ *isb.ReadMessage, writeMessagesCh chan<- isb.WriteMessage) error {
	close(writeMessagesCh)
	return fmt.Errorf("UDF error")
}

func validateMetrics(t *testing.T, batchSize int64) {
	metadata := `
		# HELP forwarder_data_read_total Total number of Data Messages Read
		# TYPE forwarder_data_read_total counter
		`
	expected := `
		forwarder_data_read_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
	`

	err := testutil.CollectAndCompare(metrics.ReadDataMessagesCount, strings.NewReader(metadata+expected), "forwarder_data_read_total")
	if err != nil {
		t.Errorf("unexpected collecting result: %v", err)
	}

	writeMetadata := `
		# HELP forwarder_write_total Total number of Messages Written
		# TYPE forwarder_write_total counter
		`
	var writeExpected string
	if batchSize > 1 {
		writeExpected = `
		forwarder_write_total{partition_name="to1-1",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%f", float64(batchSize)/2) + `
		forwarder_write_total{partition_name="to1-2",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%f", float64(batchSize)/2) + `
	`
	} else {
		writeExpected = `
		forwarder_write_total{partition_name="to1-1",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
		forwarder_write_total{partition_name="to1-2",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%f", float64(0)) + `
	`
	}

	err = testutil.CollectAndCompare(metrics.WriteMessagesCount, strings.NewReader(writeMetadata+writeExpected), "forwarder_write_total")
	if err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}

	ackMetadata := `
		# HELP forwarder_ack_total Total number of Messages Acknowledged
		# TYPE forwarder_ack_total counter
		`
	ackExpected := `
		forwarder_ack_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="MapUDF"} ` + fmt.Sprintf("%d", batchSize) + `
	`

	err = testutil.CollectAndCompare(metrics.AckMessagesCount, strings.NewReader(ackMetadata+ackExpected), "forwarder_ack_total")
	if err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func metricsReset() {
	metrics.ReadDataMessagesCount.Reset()
	metrics.WriteMessagesCount.Reset()
	metrics.AckMessagesCount.Reset()
}

func buildWatermarkStores(toBuffers map[string][]isb.BufferWriter) map[string]wmstore.WatermarkStore {
	var ctx = context.Background()
	watermarkStores := make(map[string]wmstore.WatermarkStore)
	for key := range toBuffers {
		store, _ := wmstore.BuildInmemWatermarkStore(ctx, fmt.Sprintf(publisherKeyspace, key))
		watermarkStores[key] = store
	}
	return watermarkStores
}

func buildPublisherMapAndOTStoreFromWmStores(toBuffers map[string][]isb.BufferWriter, wmStores map[string]wmstore.WatermarkStore) (map[string]publish.Publisher, map[string]kvs.KVStorer) {
	var ctx = context.Background()
	processorEntity := entity.NewProcessorEntity("publisherTestPod")
	publishers := make(map[string]publish.Publisher)
	otStores := make(map[string]kvs.KVStorer)

	for key, store := range wmStores {
		p := publish.NewPublish(ctx, processorEntity, store, int32(len(toBuffers[key])), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
		otStores[key] = store.OffsetTimelineStore()
	}
	return publishers, otStores
}

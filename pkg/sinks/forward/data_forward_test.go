package forward

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

const (
	testPipelineName    = "testPipeline"
	testVertexName      = "testVertex"
	testProcessorEntity = "publisherTestPod"
	publishKeyspace     = testPipelineName + "_" + testProcessorEntity + "_%s"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// testForwarderPublisher is for data_forward_test.go only
type testForwarderPublisher struct {
	Name string
	Idle bool
}

func (t *testForwarderPublisher) Close() error {
	return nil
}

func (t *testForwarderPublisher) PublishWatermark(_ wmb.Watermark, _ isb.Offset, _ int32) {
}

func (t *testForwarderPublisher) PublishIdleWatermark(_ wmb.Watermark, _ isb.Offset, _ int32) {
	t.Idle = true
}

func (t *testForwarderPublisher) GetLatestWatermark() wmb.Watermark {
	return wmb.InitialWatermark
}

// testForwardFetcher is for data_forward_test.go only
type testForwardFetcher struct {
}

// ComputeWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testForwardFetcher) ComputeWatermark(_ isb.Offset, _ int32) wmb.Watermark {
	return t.getWatermark()
}

func (t *testForwardFetcher) getWatermark() wmb.Watermark {
	return wmb.Watermark(testStartTime)
}

func (t *testForwardFetcher) ComputeHeadIdleWMB(int32) wmb.WMB {
	// won't be used
	return wmb.WMB{}
}

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func TestNewDataForward(t *testing.T) {
	var (
		testName        = "sink_forward"
		batchSize int64 = 10
	)

	t.Run(testName+"_basic", func(t *testing.T) {
		metricsReset()
		// set the buffer size to be 5 * batchSize, so we have enough space for testing
		fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
		// as of now, all of our sinkers have only 1 toBuffer
		to := simplebuffer.NewInMemoryBuffer("to", 5*batchSize, 0)
		toSteps := map[string][]isb.BufferWriter{
			"to": {to},
		}

		vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name: testVertexName,
			},
		}}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)

		fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
		fetchWatermark = &testForwardFetcher{}
		f, err := NewDataForward(vertex, fromStep, toSteps, myForwardTest{}, fetchWatermark, publishWatermark)

		assert.NoError(t, err)
		assert.False(t, to.IsFull())
		assert.True(t, to.IsEmpty())

		stopped := f.Start()
		// write some data
		_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
		assert.Equal(t, make([]error, batchSize), errs)

		testReadBatchSize := int(batchSize / 2)
		msgs := to.GetMessages(testReadBatchSize)
		for ; msgs[testReadBatchSize-1].ID == ""; msgs = to.GetMessages(testReadBatchSize) {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Fatal("expected to have messages in to buffer", ctx.Err())
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		// read some data
		readMessages, err := to.Read(ctx, int64(testReadBatchSize))
		assert.NoError(t, err, "expected no error")
		assert.Len(t, readMessages, testReadBatchSize)
		for i := 0; i < testReadBatchSize; i++ {
			assert.Equal(t, []interface{}{writeMessages[i].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
			assert.Equal(t, []interface{}{writeMessages[i].Kind}, []interface{}{readMessages[i].Kind})
			assert.Equal(t, []interface{}{writeMessages[i].Keys}, []interface{}{readMessages[i].Keys})
			assert.Equal(t, []interface{}{writeMessages[i].Body}, []interface{}{readMessages[i].Body})
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

	t.Run(testName+"_toAll", func(t *testing.T) {

		fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
		to1 := simplebuffer.NewInMemoryBuffer("to", 5*batchSize, 0)
		to2 := simplebuffer.NewInMemoryBuffer("to", 5*batchSize, 0)

		toSteps := map[string][]isb.BufferWriter{
			"to1": {to1},
			"to2": {to2},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)

		vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name: testVertexName,
			},
		}}

		fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
		fetchWatermark = &testForwardFetcher{}
		f, err := NewDataForward(vertex, fromStep, toSteps, myForwardToAllTest{}, fetchWatermark, publishWatermark)

		assert.NoError(t, err)
		assert.False(t, to1.IsFull())
		assert.False(t, to2.IsFull())
		assert.True(t, to1.IsEmpty())
		assert.True(t, to2.IsEmpty())

		stopped := f.Start()
		// write some data
		_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
		assert.Equal(t, make([]error, batchSize), errs)

		testReadBatchSize := int(batchSize / 2)
		msgs := to1.GetMessages(testReadBatchSize)
		for ; msgs[testReadBatchSize-1].ID == ""; msgs = to1.GetMessages(testReadBatchSize) {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Fatal("expected to have messages in to buffer", ctx.Err())
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		// read some data
		readMessages, err := to1.Read(ctx, int64(testReadBatchSize))
		assert.NoError(t, err, "expected no error")
		assert.Len(t, readMessages, testReadBatchSize)
		for i := 0; i < testReadBatchSize; i++ {
			assert.Equal(t, []interface{}{writeMessages[i].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
			assert.Equal(t, []interface{}{writeMessages[i].Kind}, []interface{}{readMessages[i].Kind})
			assert.Equal(t, []interface{}{writeMessages[i].Keys}, []interface{}{readMessages[i].Keys})
			assert.Equal(t, []interface{}{writeMessages[i].Body}, []interface{}{readMessages[i].Body})
		}

		msgs2 := to2.GetMessages(testReadBatchSize)
		for ; msgs2[testReadBatchSize-1].ID == ""; msgs2 = to2.GetMessages(testReadBatchSize) {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Fatal("expected to have messages in to buffer", ctx.Err())
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		// read some data
		readMessages2, err := to1.Read(ctx, int64(testReadBatchSize))
		assert.NoError(t, err, "expected no error")
		assert.Len(t, readMessages2, testReadBatchSize)
		for i := 0; i < testReadBatchSize; i++ {
			assert.Equal(t, []interface{}{writeMessages[i].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
			assert.Equal(t, []interface{}{writeMessages[i].Kind}, []interface{}{readMessages[i].Kind})
			assert.Equal(t, []interface{}{writeMessages[i].Keys}, []interface{}{readMessages[i].Keys})
			assert.Equal(t, []interface{}{writeMessages[i].Body}, []interface{}{readMessages[i].Body})
		}

		// write some data
		_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
		assert.Equal(t, make([]error, 3*batchSize), errs)

		f.Stop()
		<-stopped
	})

	t.Run(testName+"_dropAll", func(t *testing.T) {

		fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
		to1 := simplebuffer.NewInMemoryBuffer("to", 5*batchSize, 0)
		to2 := simplebuffer.NewInMemoryBuffer("to", 5*batchSize, 0)

		toSteps := map[string][]isb.BufferWriter{
			"to1": {to1},
			"to2": {to2},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)

		vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name: testVertexName,
			},
		}}

		fetchWatermark := &testForwardFetcher{}
		publishWatermark := map[string]publish.Publisher{
			"to1": &testForwarderPublisher{},
			"to2": &testForwarderPublisher{},
		}
		f, err := NewDataForward(vertex, fromStep, toSteps, myForwardDropTest{}, fetchWatermark, publishWatermark)

		assert.NoError(t, err)
		assert.False(t, to1.IsFull())
		assert.False(t, to2.IsFull())
		assert.True(t, to1.IsEmpty())
		assert.True(t, to2.IsEmpty())

		stopped := f.Start()
		// write some data
		_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
		assert.Equal(t, make([]error, batchSize), errs)

		// we don't write control message for sink, only publish idle watermark
		for !publishWatermark["to1"].(*testForwarderPublisher).Idle {
			select {
			case <-ctx.Done():
				logging.FromContext(ctx).Fatalf("expect to have the idle wm in to1, %s", ctx.Err())
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
		for !publishWatermark["to2"].(*testForwarderPublisher).Idle {
			select {
			case <-ctx.Done():
				logging.FromContext(ctx).Fatalf("expect to have the idle wm in to2, %s", ctx.Err())
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}

		// since this is a dropping WhereTo, the buffer can never be full
		f.Stop()

		<-stopped
	})
	// // Explicitly tests the case where we forward to only one buffer
	// t.Run(testName+"_toOneStep", func(t *testing.T) {
	//
	// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
	// 	to11 := simplebuffer.NewInMemoryBuffer("to1-1", 2*batchSize, 0)
	// 	to12 := simplebuffer.NewInMemoryBuffer("to1-2", 2*batchSize, 1)
	// 	to21 := simplebuffer.NewInMemoryBuffer("to2-1", 2*batchSize, 0)
	// 	to22 := simplebuffer.NewInMemoryBuffer("to2-2", 2*batchSize, 1)
	// 	toSteps := map[string][]isb.BufferWriter{
	// 		"to1": {to11, to12},
	// 		"to2": {to21, to22},
	// 	}
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	defer cancel()
	//
	// 	writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)
	//
	// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
	// 		PipelineName: "testPipeline",
	// 		AbstractVertex: dfv1.AbstractVertex{
	// 			Name: "testVertex",
	// 		},
	// 	}}
	//
	// 	fetchWatermark := &testForwardFetcher{}
	// 	toVertexStores := buildToVertexWatermarkStores(toSteps)
	//
	// 	f, err := NewDataForward(vertex, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, myForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(batchSize))
	//
	// 	assert.NoError(t, err)
	// 	assert.False(t, to11.IsFull())
	// 	assert.False(t, to12.IsFull())
	//
	// 	assert.True(t, to11.IsEmpty())
	// 	assert.True(t, to12.IsEmpty())
	//
	// 	stopped := f.Start()
	// 	// write some data
	// 	_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
	// 	assert.Equal(t, make([]error, batchSize), errs)
	// 	var updatedBatchSize int
	// 	if batchSize > 1 {
	// 		updatedBatchSize = int(batchSize / 2)
	// 	} else {
	// 		updatedBatchSize = int(batchSize)
	// 	}
	// 	// read some data
	// 	readMessages, err := to11.Read(ctx, int64(updatedBatchSize))
	// 	assert.NoError(t, err, "expected no error")
	//
	// 	assert.Len(t, readMessages, updatedBatchSize)
	// 	for i, j := 0, 0; i < updatedBatchSize; i, j = i+1, j+2 {
	// 		assert.Equal(t, []interface{}{writeMessages[j].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
	// 		assert.Equal(t, []interface{}{writeMessages[j].Kind}, []interface{}{readMessages[i].Kind})
	// 		assert.Equal(t, []interface{}{writeMessages[j].Keys}, []interface{}{readMessages[i].Keys})
	// 		assert.Equal(t, []interface{}{writeMessages[j].Body}, []interface{}{readMessages[i].Body})
	// 	}
	// 	// write some data
	// 	_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
	// 	assert.Equal(t, make([]error, 3*batchSize), errs)
	//
	// 	var wg sync.WaitGroup
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		otKeys1, _ := toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
	// 	loop:
	// 		for {
	// 			select {
	// 			case <-ctx.Done():
	// 				assert.Fail(t, "context cancelled while waiting to get a valid watermark")
	// 				break loop
	// 			default:
	// 				if len(otKeys1) == 0 {
	// 					otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
	// 					time.Sleep(time.Millisecond * 10)
	// 				} else {
	// 					// NOTE: in this test we only have one processor to publish
	// 					// so len(otKeys) should always be 1
	// 					otKeys1, _ = toVertexStores["to1"].OffsetTimelineStore().GetAllKeys(ctx)
	// 					otValue1, _ := toVertexStores["to1"].OffsetTimelineStore().GetValue(ctx, otKeys1[0])
	// 					otDecode1, _ := wmb.DecodeToWMB(otValue1)
	// 					if batchSize > 1 && !otDecode1.Idle {
	// 						break loop
	// 					} else if batchSize == 1 && otDecode1.Idle {
	// 						break loop
	// 					} else {
	// 						time.Sleep(time.Millisecond * 10)
	// 					}
	// 				}
	// 			}
	//
	// 		}
	// 	}()
	//
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		otKeys2, _ := toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
	// 	loop:
	// 		for {
	// 			select {
	// 			case <-ctx.Done():
	// 				assert.Fail(t, "context cancelled while waiting to get a valid watermark")
	// 				break loop
	// 			default:
	// 				if len(otKeys2) == 0 {
	// 					otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
	// 					time.Sleep(time.Millisecond * 10)
	// 				} else {
	// 					// NOTE: in this test we only have one processor to publish
	// 					// so len(otKeys) should always be 1
	// 					otKeys2, _ = toVertexStores["to2"].OffsetTimelineStore().GetAllKeys(ctx)
	// 					otValue2, _ := toVertexStores["to2"].OffsetTimelineStore().GetValue(ctx, otKeys2[0])
	// 					otDecode2, _ := wmb.DecodeToWMB(otValue2)
	// 					// if the batch size is 1, then the watermark should be idle because
	// 					// one of partitions will be idle
	// 					if otDecode2.Idle {
	// 						break loop
	// 					} else {
	// 						time.Sleep(time.Millisecond * 10)
	// 					}
	// 				}
	// 			}
	//
	// 		}
	// 	}()
	// 	wg.Wait()
	//
	// 	// stop will cancel the contexts and therefore the forwarder stops without waiting
	// 	f.Stop()
	//
	// 	<-stopped
	// })
	// // Test the scenario with Transformer error
	// t.Run(testName+"_TransformerError", func(t *testing.T) {
	//
	// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
	// 	to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
	// 	toSteps := map[string][]isb.BufferWriter{
	// 		"to1": {to1},
	// 	}
	//
	// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
	// 		PipelineName: "testPipeline",
	// 		AbstractVertex: dfv1.AbstractVertex{
	// 			Name: "testVertex",
	// 		},
	// 	}}
	//
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	defer cancel()
	//
	// 	writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)
	//
	// 	fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	// 	toVertexStores := buildNoOpToVertexStores(toSteps)
	// 	f, err := NewDataForward(vertex, fromStep, toSteps, myForwardApplyTransformerErrTest{}, myForwardApplyTransformerErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(batchSize))
	//
	// 	assert.NoError(t, err)
	// 	assert.False(t, to1.IsFull())
	// 	assert.True(t, to1.IsEmpty())
	//
	// 	stopped := f.Start()
	// 	// write some data
	// 	_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
	// 	assert.Equal(t, make([]error, batchSize), errs)
	// 	assert.True(t, to1.IsEmpty())
	//
	// 	f.Stop()
	// 	time.Sleep(1 * time.Millisecond)
	//
	// 	<-stopped
	// })
	// // Test the scenario with error
	// t.Run(testName+"_whereToError", func(t *testing.T) {
	//
	// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
	// 	to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
	// 	toSteps := map[string][]isb.BufferWriter{
	// 		"to1": {to1},
	// 	}
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	defer cancel()
	//
	// 	writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)
	//
	// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
	// 		PipelineName: "testPipeline",
	// 		AbstractVertex: dfv1.AbstractVertex{
	// 			Name: "testVertex",
	// 		},
	// 	}}
	//
	// 	fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	// 	toVertexStores := buildNoOpToVertexStores(toSteps)
	// 	f, err := NewDataForward(vertex, fromStep, toSteps, myForwardApplyWhereToErrTest{}, myForwardApplyWhereToErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(batchSize))
	//
	// 	assert.NoError(t, err)
	// 	assert.True(t, to1.IsEmpty())
	//
	// 	stopped := f.Start()
	// 	// write some data
	// 	_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
	// 	assert.Equal(t, make([]error, batchSize), errs)
	//
	// 	f.Stop()
	// 	time.Sleep(1 * time.Millisecond)
	//
	// 	assert.True(t, to1.IsEmpty())
	// 	<-stopped
	// })
	// t.Run(testName+"_withInternalError", func(t *testing.T) {
	// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
	// 	to1 := simplebuffer.NewInMemoryBuffer("to1", 2*batchSize, 0)
	// 	toSteps := map[string][]isb.BufferWriter{
	// 		"to1": {to1},
	// 	}
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	defer cancel()
	//
	// 	writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime)
	//
	// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
	// 		PipelineName: "testPipeline",
	// 		AbstractVertex: dfv1.AbstractVertex{
	// 			Name: "testVertex",
	// 		},
	// 	}}
	//
	// 	fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	// 	toVertexStores := buildNoOpToVertexStores(toSteps)
	// 	f, err := NewDataForward(vertex, fromStep, toSteps, myForwardInternalErrTest{}, myForwardInternalErrTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(batchSize))
	//
	// 	assert.NoError(t, err)
	// 	assert.False(t, to1.IsFull())
	// 	assert.True(t, to1.IsEmpty())
	//
	// 	stopped := f.Start()
	// 	// write some data
	// 	_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
	// 	assert.Equal(t, make([]error, batchSize), errs)
	//
	// 	f.Stop()
	// 	time.Sleep(1 * time.Millisecond)
	// 	<-stopped
	// })
}

// mySourceForwardTest tests source data transformer by updating message event time, and then verifying new event time and IsLate assignments.
type mySourceForwardTest struct {
}

func (f mySourceForwardTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

type mySourceForwardTestRoundRobin struct {
	count int
}

func (f *mySourceForwardTestRoundRobin) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	var output = []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: int32(f.count % 2),
	}}
	f.count++
	return output, nil
}

// func TestDataForwardSinglePartition(t *testing.T) {
// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
// 	to1 := simplebuffer.NewInMemoryBuffer("to1", 10, 0, simplebuffer.WithReadTimeOut(time.Second*10))
// 	toSteps := map[string][]isb.BufferWriter{
// 		"to1": {to1},
// 	}
// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
// 		PipelineName: "testPipeline",
// 		AbstractVertex: dfv1.AbstractVertex{
// 			Name: "receivingVertex",
// 		},
// 	}}
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	defer cancel()
//
// 	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
// 	fetchWatermark := &testForwardFetcher{}
// 	toVertexStores := buildNoOpToVertexStores(toSteps)
//
// 	f, err := NewDataForward(vertex, fromStep, toSteps, mySourceForwardTest{}, mySourceForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(5))
// 	assert.NoError(t, err)
// 	assert.False(t, to1.IsFull())
// 	assert.True(t, to1.IsEmpty())
//
// 	stopped := f.Start()
// 	count := int64(2)
// 	// write some data
// 	_, errs := fromStep.Write(ctx, writeMessages[0:count])
// 	assert.Equal(t, make([]error, count), errs)
//
// 	// read some data
// 	readMessages, err := to1.Read(ctx, count)
// 	assert.NoError(t, err, "expected no error")
// 	assert.Len(t, readMessages, int(count))
// 	assert.Equal(t, []interface{}{writeMessages[0].Header.Keys, writeMessages[1].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
// 	assert.Equal(t, []interface{}{"0-0-receivingVertex-0", "1-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
// 	for _, m := range readMessages {
// 		// verify new event time gets assigned to messages.
// 		assert.Equal(t, testSourceNewEventTime, m.EventTime)
// 		// verify messages are marked as late because of event time update.
// 		assert.Equal(t, true, m.IsLate)
// 	}
// 	f.Stop()
// 	time.Sleep(1 * time.Millisecond)
// 	// only for shutdown will work as from buffer is not empty
// 	f.ForceStop()
// 	<-stopped
// }
//
// func TestDataForwardMultiplePartition(t *testing.T) {
// 	fromStep := simplebuffer.NewInMemoryBuffer("from", 25, 0)
// 	to11 := simplebuffer.NewInMemoryBuffer("to1-0", 10, 0, simplebuffer.WithReadTimeOut(time.Second*10))
// 	to12 := simplebuffer.NewInMemoryBuffer("to1-1", 10, 1, simplebuffer.WithReadTimeOut(time.Second*10))
// 	toSteps := map[string][]isb.BufferWriter{
// 		"to1": {to11, to12},
// 	}
// 	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
// 		PipelineName: "testPipeline",
// 		AbstractVertex: dfv1.AbstractVertex{
// 			Name: "receivingVertex",
// 		},
// 	}}
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	defer cancel()
//
// 	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
// 	fetchWatermark := &testForwardFetcher{}
// 	toVertexStores := buildNoOpToVertexStores(toSteps)
//
// 	f, err := NewDataForward(vertex, fromStep, toSteps, &mySourceForwardTestRoundRobin{}, mySourceForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(5))
// 	assert.NoError(t, err)
// 	assert.False(t, to11.IsFull())
// 	assert.False(t, to12.IsFull())
// 	assert.True(t, to11.IsEmpty())
// 	assert.True(t, to12.IsEmpty())
//
// 	stopped := f.Start()
// 	count := int64(4)
// 	// write some data
// 	_, errs := fromStep.Write(ctx, writeMessages[0:count])
// 	assert.Equal(t, make([]error, count), errs)
//
// 	time.Sleep(time.Second)
// 	// read some data
// 	// since we have produced four messages, both the partitions should have two messages each.(we write in round-robin fashion)
// 	readMessages, err := to11.Read(ctx, 2)
// 	assert.NoError(t, err, "expected no error")
// 	assert.Len(t, readMessages, 2)
// 	assert.Equal(t, []interface{}{writeMessages[0].Header.Keys, writeMessages[2].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
// 	assert.Equal(t, []interface{}{"0-0-receivingVertex-0", "2-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
// 	for _, m := range readMessages {
// 		// verify new event time gets assigned to messages.
// 		assert.Equal(t, testSourceNewEventTime, m.EventTime)
// 		// verify messages are marked as late because of event time update.
// 		assert.Equal(t, true, m.IsLate)
// 	}
//
// 	time.Sleep(time.Second)
//
// 	readMessages, err = to12.Read(ctx, 2)
// 	assert.NoError(t, err, "expected no error")
// 	assert.Len(t, readMessages, 2)
// 	assert.Equal(t, []interface{}{writeMessages[1].Header.Keys, writeMessages[3].Header.Keys}, []interface{}{readMessages[0].Header.Keys, readMessages[1].Header.Keys})
// 	assert.Equal(t, []interface{}{"1-0-receivingVertex-0", "3-0-receivingVertex-0"}, []interface{}{readMessages[0].Header.ID, readMessages[1].Header.ID})
// 	for _, m := range readMessages {
// 		// verify new event time gets assigned to messages.
// 		assert.Equal(t, testSourceNewEventTime, m.EventTime)
// 		// verify messages are marked as late because of event time update.
// 		assert.Equal(t, true, m.IsLate)
// 	}
// 	f.Stop()
// 	time.Sleep(1 * time.Millisecond)
// 	// only for shutdown will work as from buffer is not empty
// 	f.ForceStop()
// 	<-stopped
// }
//
// // TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
// func TestWriteToBuffer(t *testing.T) {
// 	tests := []struct {
// 		name       string
// 		batchSize  int64
// 		strategy   dfv1.BufferFullWritingStrategy
// 		throwError bool
// 	}{
// 		{
// 			name:      "test-discard-latest",
// 			batchSize: 10,
// 			strategy:  dfv1.DiscardLatest,
// 			// should not throw any error as we drop messages and finish writing before context is cancelled
// 			throwError: false,
// 		},
// 		{
// 			name:      "test-retry-until-success",
// 			batchSize: 10,
// 			strategy:  dfv1.RetryUntilSuccess,
// 			// should throw context closed error as we keep retrying writing until context is cancelled
// 			throwError: true,
// 		},
// 		{
// 			name:      "test-discard-latest",
// 			batchSize: 1,
// 			strategy:  dfv1.DiscardLatest,
// 			// should not throw any error as we drop messages and finish writing before context is cancelled
// 			throwError: false,
// 		},
// 		{
// 			name:      "test-retry-until-success",
// 			batchSize: 1,
// 			strategy:  dfv1.RetryUntilSuccess,
// 			// should throw context closed error as we keep retrying writing until context is cancelled
// 			throwError: true,
// 		},
// 	}
// 	for _, value := range tests {
// 		t.Run(value.name, func(t *testing.T) {
// 			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*value.batchSize, 0)
// 			buffer := simplebuffer.NewInMemoryBuffer("to1", value.batchSize, 0, simplebuffer.WithBufferFullWritingStrategy(value.strategy))
// 			toSteps := map[string][]isb.BufferWriter{
// 				"to1": {buffer},
// 			}
// 			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 			defer cancel()
// 			vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
// 				PipelineName: "testPipeline",
// 				AbstractVertex: dfv1.AbstractVertex{
// 					Name: "testVertex",
// 				},
// 			}}
//
// 			fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
// 			toVertexStores := buildNoOpToVertexStores(toSteps)
// 			f, err := NewDataForward(vertex, fromStep, toSteps, myForwardTest{}, myForwardTest{}, fetchWatermark, TestSourceWatermarkPublisher{}, toVertexStores, WithReadBatchSize(value.batchSize))
// 			assert.NoError(t, err)
// 			assert.False(t, buffer.IsFull())
// 			assert.True(t, buffer.IsEmpty())
//
// 			stopped := f.Start()
// 			go func() {
// 				for !buffer.IsFull() {
// 					select {
// 					case <-ctx.Done():
// 						logging.FromContext(ctx).Fatalf("not full, %s", ctx.Err())
// 					default:
// 						time.Sleep(1 * time.Millisecond)
// 					}
// 				}
// 				// stop will cancel the context
// 				f.Stop()
// 			}()
//
// 			// try to write to buffer after it is full.
// 			var messageToStep = make(map[string][][]isb.Message)
// 			messageToStep["to1"] = make([][]isb.Message, 1)
// 			writeMessages := testutils.BuildTestWriteMessages(4*value.batchSize, testStartTime)
// 			messageToStep["to1"][0] = append(messageToStep["to1"][0], writeMessages[0:value.batchSize+1]...)
// 			_, err = f.writeToBuffers(ctx, messageToStep)
//
// 			assert.Equal(t, value.throwError, err != nil)
// 			if value.throwError {
// 				// assert the number of failed messages
// 				assert.True(t, strings.Contains(err.Error(), "with failed messages:1"))
// 			}
// 			<-stopped
// 		})
// 	}
// }

type myForwardDropTest struct {
}

func (f myForwardDropTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{}, nil
}

func (f myForwardDropTest) ApplyTransform(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

type myForwardToAllTest struct {
	count int
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	var output = []forward.VertexBuffer{{
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

type myForwardInternalErrTest struct {
}

func (f myForwardInternalErrTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, nil
}

type myForwardApplyWhereToErrTest struct {
}

func (f myForwardApplyWhereToErrTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "to1",
		ToVertexPartitionIdx: 0,
	}}, fmt.Errorf("whereToStep failed")
}

func validateMetrics(batchSize int64) (err error) {
	metadata := `
		# HELP sink_forwarder_read_total Total number of Messages Read
		# TYPE sink_forwarder_read_total counter
		`
	expected := `
		sink_forwarder_read_total{partition_name="from",pipeline="testPipeline",vertex="testVertex"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
	`

	err = testutil.CollectAndCompare(readMessagesCount, strings.NewReader(metadata+expected), "sink_forwarder_read_total")
	if err != nil {
		return err
	}

	writeMetadata := `
		# HELP sink_forwarder_write_total Total number of Messages Written
		# TYPE sink_forwarder_write_total counter
		`
	var writeExpected string
	writeExpected = `
		sink_forwarder_write_total{partition_name="to",pipeline="testPipeline",vertex="testVertex"} ` + fmt.Sprintf("%d", batchSize) + `
	`
	err = testutil.CollectAndCompare(writeMessagesCount, strings.NewReader(writeMetadata+writeExpected), "sink_forwarder_write_total")
	if err != nil {
		return err
	}

	ackMetadata := `
		# HELP sink_forwarder_ack_total Total number of Messages Acknowledged
		# TYPE sink_forwarder_ack_total counter
		`
	ackExpected := `
		sink_forwarder_ack_total{partition_name="from",pipeline="testPipeline",vertex="testVertex"} ` + fmt.Sprintf("%d", batchSize) + `
	`

	err = testutil.CollectAndCompare(ackMessagesCount, strings.NewReader(ackMetadata+ackExpected), "sink_forwarder_ack_total")
	if err != nil {
		return err
	}

	return nil
}

func metricsReset() {
	readMessagesCount.Reset()
	writeMessagesCount.Reset()
	ackMessagesCount.Reset()
}

// buildPublisherMap builds OTStore and publisher for each toBuffer
func buildToVertexWatermarkStores(toBuffers map[string][]isb.BufferWriter) map[string]wmstore.WatermarkStore {
	var ctx = context.Background()
	otStores := make(map[string]wmstore.WatermarkStore)
	for key := range toBuffers {
		store, _, _, _ := wmstore.BuildInmemWatermarkStore(ctx, fmt.Sprintf(publishKeyspace, key))
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

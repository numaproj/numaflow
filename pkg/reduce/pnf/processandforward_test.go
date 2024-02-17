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

package pnf

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1/reducemock"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window/keyed"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	udfcall "github.com/numaproj/numaflow/pkg/udf/rpc"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publisherKeyspace   = testPipelineName + "_" + testProcessorEntity + "_%s"
)

type forwardTest struct {
	count   int
	buffers []string
}

func (f *forwardTest) WhereTo(keys []string, _ []string) ([]forwarder.VertexBuffer, error) {
	if strings.Compare(keys[len(keys)-1], "test-forward-one") == 0 {
		return []forwarder.VertexBuffer{{
			ToVertexName:         "buffer1",
			ToVertexPartitionIdx: int32(f.count % 2),
		}}, nil
	} else if strings.Compare(keys[len(keys)-1], "test-forward-all") == 0 {
		var steps []forwarder.VertexBuffer
		for _, buffer := range f.buffers {
			steps = append(steps, forwarder.VertexBuffer{
				ToVertexName:         buffer,
				ToVertexPartitionIdx: int32(f.count % 2),
			})
		}
		return steps, nil
	}
	f.count++
	return []forwarder.VertexBuffer{}, nil
}

func (f forwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

type PayloadForTest struct {
	Key   string
	Value int64
}

func TestProcessAndForward_Process(t *testing.T) {
	// 1. create a pbq which has to be passed to the process method
	// 2. pass the pbqReader interface and create a new processAndForward instance
	// 3. mock the grpc client methods
	// 4. assert to check if the process method is returning the writeMessages

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	testPartition := partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Slot:  "partition-1",
	}
	kw := keyed.NewKeyedWindow(time.UnixMilli(60000), time.UnixMilli(120000))
	kw.AddSlot("partition-1")

	var err error
	var pbqManager *pbq.Manager

	pbqManager, err = pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(memory.WithStoreSize(100)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a pbq for a partition
	var simplePbq pbq.ReadWriteCloser
	simplePbq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	// write messages to pbq
	go func() {
		writeMessages := testutils.BuildTestReadMessages(10, time.Now())
		for index := range writeMessages {
			err := simplePbq.Write(ctx, &writeMessages[index])
			assert.NoError(t, err)
		}
		// done writing, cob
		simplePbq.CloseOfBook()
	}()

	// mock grpc reducer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := reducemock.NewMockReduceClient(ctrl)
	mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Results: []*reducepb.ReduceResponse_Result{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, nil).Times(1)
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Results: []*reducepb.ReduceResponse_Result{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, io.EOF).Times(1)

	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	c, _ := reducer.NewFromClient(mockClient)
	client := udfcall.NewUDSgRPCBasedReduce(c, "test", 0)

	assert.NoError(t, err)
	_, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(make(map[string][]isb.BufferWriter))

	// create pf using key and reducer
	pf := newProcessAndForward(ctx, "reduce", "test-pipeline", 0, testPartition, client, simplePbq, make(map[string][]isb.BufferWriter, 1), &forwardTest{}, publishWatermark, wmb.NewIdleManager(1))

	err = pf.Process(ctx)
	assert.NoError(t, err)
	assert.Len(t, pf.writeMessages, 1)
}

func TestProcessAndForward_Forward(t *testing.T) {
	ctx := context.Background()

	var pbqManager *pbq.Manager

	pbqManager, _ = pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores())

	test1Buffer11 := simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0)
	test1Buffer12 := simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1)

	test1Buffer21 := simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0)
	test1Buffer22 := simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 1)

	toBuffers1 := map[string][]isb.BufferWriter{
		"buffer1": {test1Buffer11, test1Buffer12},
		"buffer2": {test1Buffer21, test1Buffer22},
	}

	pf1, otStores1 := createProcessAndForwardAndOTStore(ctx, "test-forward-one", pbqManager, toBuffers1)

	test2Buffer11 := simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0)
	test2Buffer12 := simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1)

	test2Buffer21 := simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0)
	test2Buffer22 := simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 1)

	toBuffers2 := map[string][]isb.BufferWriter{
		"buffer1": {test2Buffer11, test2Buffer12},
		"buffer2": {test2Buffer21, test2Buffer22},
	}

	pf2, otStores2 := createProcessAndForwardAndOTStore(ctx, "test-forward-all", pbqManager, toBuffers2)

	test3Buffer11 := simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0)
	test3buffer12 := simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1)

	test3Buffer21 := simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0)
	test3Buffer22 := simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 1)

	toBuffers3 := map[string][]isb.BufferWriter{
		"buffer1": {test3Buffer11, test3buffer12},
		"buffer2": {test3Buffer21, test3Buffer22},
	}

	pf3, otStores3 := createProcessAndForwardAndOTStore(ctx, "test-drop-all", pbqManager, toBuffers3)

	tests := []struct {
		name       string
		id         partition.ID
		buffers    []*simplebuffer.InMemoryBuffer
		pf         processAndForward
		otStores   map[string]kvs.KVStorer
		expected   []bool
		wmExpected map[string]wmb.WMB
	}{
		{
			name: "test-forward-one",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Slot:  "test-forward-one",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test1Buffer11, test1Buffer12, test1Buffer21, test1Buffer22},
			pf:       pf1,
			otStores: otStores1,
			expected: []bool{false, true, true, true}, // should have one ctrl message for buffer2
			wmExpected: map[string]wmb.WMB{
				"buffer1": {
					Offset:    0,
					Partition: 1,
					Watermark: int64(119999),
					Idle:      true,
				},
				"buffer2": {
					Offset:    0,
					Partition: 1,
					Watermark: int64(119999),
					Idle:      true,
				},
			},
		},
		{
			name: "test-forward-all",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Slot:  "test-forward-all",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test2Buffer11, test2Buffer12, test2Buffer21, test2Buffer22},
			pf:       pf2,
			otStores: otStores2,
			expected: []bool{false, true, false, true},
			wmExpected: map[string]wmb.WMB{
				"buffer1": {
					Offset:    0,
					Partition: 1,
					Watermark: int64(119999),
					Idle:      true,
				},
				"buffer2": {
					Offset:    0,
					Partition: 1,
					Watermark: int64(119999),
					Idle:      true,
				},
			},
		},
		{
			name: "test-drop-all",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Slot:  "test-drop-all",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test3Buffer11, test3buffer12, test3Buffer21, test3Buffer22},
			pf:       pf3,
			otStores: otStores3,
			expected: []bool{true, true, true, true}, // should have one ctrl message for each buffer
			wmExpected: map[string]wmb.WMB{
				"buffer1": {
					Partition: 1,
					Offset:    0,
					Watermark: int64(119999),
					Idle:      true,
				},
				"buffer2": {
					Partition: 1,
					Offset:    0,
					Watermark: int64(119999),
					Idle:      true,
				},
			},
		},
	}

	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			err := value.pf.Forward(ctx)
			assert.NoError(t, err)
			msgs0, err := value.buffers[0].Read(ctx, 1)
			assert.NoError(t, err)
			assert.Equal(t, value.expected[0], msgs0[0].Header.Kind == isb.WMB)
			msgs1, err := value.buffers[1].Read(ctx, 1)
			assert.NoError(t, err)
			assert.Equal(t, value.expected[1], msgs1[0].Header.Kind == isb.WMB)
			msgs2, err := value.buffers[2].Read(ctx, 1)
			assert.NoError(t, err)
			assert.Equal(t, value.expected[2], msgs2[0].Header.Kind == isb.WMB)
			msgs3, err := value.buffers[3].Read(ctx, 1)
			assert.NoError(t, err)
			assert.Equal(t, value.expected[3], msgs3[0].Header.Kind == isb.WMB)
			// pbq entry from the manager will be removed after forwarding
			assert.Equal(t, nil, pbqManager.GetPBQ(value.id))
			for bufferName := range value.pf.wmPublishers {
				// NOTE: in this test we only have one processor to publish
				// so len(otKeys) should always be 1
				otKeys, _ := value.otStores[bufferName].GetAllKeys(ctx)
				for _, otKey := range otKeys {
					otValue, _ := value.otStores[bufferName].GetValue(ctx, otKey)
					ot, _ := wmb.DecodeToWMB(otValue)
					assert.Equal(t, value.wmExpected[bufferName], ot)
				}
			}
		})
	}
}

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	tests := []struct {
		name       string
		buffers    []isb.BufferWriter
		throwError bool
	}{
		{
			name: "test-discard-latest",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.DiscardLatest)),
				simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.DiscardLatest))},
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name: "test-retry-until-success",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.RetryUntilSuccess)),
				simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.RetryUntilSuccess))},
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			testStartTime := time.Unix(1636470000, 0).UTC()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var pbqManager *pbq.Manager
			pbqManager, _ = pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores())
			toBuffer := map[string][]isb.BufferWriter{
				"buffer": value.buffers,
			}
			pf, _ := createProcessAndForwardAndOTStore(ctx, value.name, pbqManager, toBuffer)
			var err error
			writeMessages := testutils.BuildTestWriteMessages(int64(15), testStartTime)
			_, err = pf.writeToBuffer(ctx, "buffer", 0, writeMessages)
			assert.Equal(t, value.throwError, err != nil)
		})
	}
}

func createProcessAndForwardAndOTStore(ctx context.Context, key string, pbqManager *pbq.Manager, toBuffers map[string][]isb.BufferWriter) (processAndForward, map[string]kvs.KVStorer) {

	testPartition := partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Slot:  key,
	}
	kw := keyed.NewKeyedWindow(time.UnixMilli(60000), time.UnixMilli(120000))
	kw.AddSlot(key)

	// create a pbq for a partition
	pw, otStore := buildPublisherMapAndOTStore(toBuffers)
	var simplePbq pbq.Reader
	simplePbq, _ = pbqManager.CreateNewPBQ(ctx, testPartition)

	resultPayload, _ := json.Marshal(PayloadForTest{
		Key:   "result_payload",
		Value: 100,
	})

	var result = []*isb.WriteMessage{
		{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.UnixMilli(60000),
					},
					ID:   "1",
					Keys: []string{key},
				},
				Body: isb.Body{Payload: resultPayload},
			},
			Tags: []string{key},
		},
	}

	buffers := make([]string, 0)
	for k := range toBuffers {
		buffers = append(buffers, k)
	}
	whereto := &forwardTest{
		buffers: buffers,
	}

	pf := processAndForward{
		PartitionID:    testPartition,
		UDF:            nil,
		writeMessages:  result,
		pbqReader:      simplePbq,
		log:            logging.FromContext(ctx),
		toBuffers:      toBuffers,
		whereToDecider: whereto,
		wmPublishers:   pw,
		idleManager:    wmb.NewIdleManager(len(toBuffers)),
	}

	return pf, otStore
}

// buildPublisherMapAndOTStore builds OTStore and publisher for each toBuffer
func buildPublisherMapAndOTStore(toBuffers map[string][]isb.BufferWriter) (map[string]publish.Publisher, map[string]kvs.KVStorer) {
	var ctx = context.Background()
	processorEntity := entity.NewProcessorEntity("publisherTestPod")
	publishers := make(map[string]publish.Publisher)
	otStores := make(map[string]kvs.KVStorer)
	for key, partitionedBuffers := range toBuffers {
		store, _ := wmstore.BuildInmemWatermarkStore(ctx, publisherKeyspace)
		otStores[key] = store.OffsetTimelineStore()
		p := publish.NewPublish(ctx, processorEntity, store, int32(len(partitionedBuffers)), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	return publishers, otStores
}

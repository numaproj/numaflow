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
	"strings"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"

	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/numaproj/numaflow-go/pkg/function/clienttest"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	udfcall "github.com/numaproj/numaflow/pkg/udf/function"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/stretchr/testify/assert"
)

type myForwardTest struct {
}

func (f myForwardTest) WhereTo(key string) ([]string, error) {
	if strings.Compare(key, "test-forward-one") == 0 {
		return []string{"buffer1"}, nil
	} else if strings.Compare(key, "test-forward-all") == 0 {
		return []string{dfv1.MessageKeyAll}, nil
	}
	return []string{dfv1.MessageKeyDrop}, nil
}

func (f myForwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

type PayloadForTest struct {
	Key   string
	Value int64
}

func TestProcessAndForward_Process(t *testing.T) {
	// 1. create a pbq which has to be passed to the process method
	// 2. pass the pbqReader interface and create a new ProcessAndForward instance
	// 3. mock the grpc client methods
	// 4. assert to check if the process method is returning the result

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	size := 100
	testPartition := partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Key:   "partition-1",
	}
	var err error
	var pbqManager *pbq.Manager

	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
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

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockReduceClient := udfcall.NewMockUserDefinedFunction_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseAndRecv().Return(&functionpb.DatumList{
		Elements: []*functionpb.Datum{
			{
				Key:   "reduced_result_key",
				Value: []byte(`forward_message`),
			},
		},
	}, nil)

	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	c, _ := clienttest.New(mockClient)
	client := udfcall.NewUdsGRPCBasedUDFWithClient(c)

	assert.NoError(t, err)
	_, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(make(map[string]isb.BufferWriter))
	// create pf using key and reducer
	pf := NewProcessAndForward(ctx, testPartition, client, simplePbq, make(map[string]isb.BufferWriter), myForwardTest{}, publishWatermark)

	err = pf.Process(ctx)
	assert.NoError(t, err)
	assert.Len(t, pf.result, 1)
}

func TestProcessAndForward_Forward(t *testing.T) {
	ctx := context.Background()

	var pbqManager *pbq.Manager

	pbqManager, _ = pbq.NewManager(ctx)

	test1Buffer1 := simplebuffer.NewInMemoryBuffer("buffer1", 10)
	test1Buffer2 := simplebuffer.NewInMemoryBuffer("buffer2", 10)

	toBuffers1 := map[string]isb.BufferWriter{
		"buffer1": test1Buffer1,
		"buffer2": test1Buffer2,
	}

	test2Buffer1 := simplebuffer.NewInMemoryBuffer("buffer1", 10)
	test2Buffer2 := simplebuffer.NewInMemoryBuffer("buffer2", 10)

	toBuffers2 := map[string]isb.BufferWriter{
		"buffer1": test2Buffer1,
		"buffer2": test2Buffer2,
	}

	test3Buffer1 := simplebuffer.NewInMemoryBuffer("buffer1", 10)
	test3Buffer2 := simplebuffer.NewInMemoryBuffer("buffer2", 10)

	toBuffers3 := map[string]isb.BufferWriter{
		"buffer1": test3Buffer1,
		"buffer2": test3Buffer2,
	}

	tests := []struct {
		name       string
		id         partition.ID
		buffers    []*simplebuffer.InMemoryBuffer
		pf         ProcessAndForward
		expected   []bool
		wmExpected map[string]int64
	}{
		{
			name: "test-forward-one",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Key:   "test-forward-one",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test1Buffer1, test1Buffer2},
			pf:       createProcessAndForward(ctx, "test-forward-one", pbqManager, toBuffers1),
			expected: []bool{false, true},
			wmExpected: map[string]int64{
				"buffer1": 120000,
				"buffer2": -1,
			},
		},
		{
			name: "test-forward-all",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Key:   "test-forward-all",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test2Buffer1, test2Buffer2},
			pf:       createProcessAndForward(ctx, "test-forward-all", pbqManager, toBuffers2),
			expected: []bool{false, false},
			wmExpected: map[string]int64{
				"buffer1": 120000,
				"buffer2": 120000,
			},
		},
		{
			name: "test-drop-all",
			id: partition.ID{
				Start: time.UnixMilli(60000),
				End:   time.UnixMilli(120000),
				Key:   "test-drop-all",
			},
			buffers:  []*simplebuffer.InMemoryBuffer{test3Buffer1, test3Buffer2},
			pf:       createProcessAndForward(ctx, "test-drop-all", pbqManager, toBuffers3),
			expected: []bool{true, true},
			wmExpected: map[string]int64{
				"buffer1": -1,
				"buffer2": -1,
			},
		},
	}

	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			err := value.pf.Forward(ctx)
			assert.NoError(t, err)
			assert.Equal(t, []bool{value.buffers[0].IsEmpty(), value.buffers[1].IsEmpty()}, value.expected)
			// pbq entry from the manager will be removed after forwarding
			assert.Equal(t, pbqManager.GetPBQ(value.id), nil)
			index := 0
			for k, v := range value.pf.publishWatermark {
				// expected watermark should be equal to window end time
				assert.Equal(t, v.GetLatestWatermark().UnixMilli(), value.wmExpected[k])
				index += 1
			}
		})
	}
}

func createProcessAndForward(ctx context.Context, key string, pbqManager *pbq.Manager, toBuffers map[string]isb.BufferWriter) ProcessAndForward {

	testPartition := partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Key:   key,
	}

	// create a pbq for a partition
	pw := buildPublisherMap(toBuffers)
	var simplePbq pbq.Reader
	simplePbq, _ = pbqManager.CreateNewPBQ(ctx, testPartition)

	resultPayload, _ := json.Marshal(PayloadForTest{
		Key:   "result_payload",
		Value: 100,
	})

	var result = []*isb.Message{
		{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{
					EventTime: time.UnixMilli(60000),
				},
				ID: "1",
			},
			Body: isb.Body{Payload: resultPayload},
		},
	}

	pf := ProcessAndForward{
		PartitionID:      testPartition,
		UDF:              nil,
		result:           result,
		pbqReader:        simplePbq,
		log:              nil,
		toBuffers:        toBuffers,
		whereToDecider:   myForwardTest{},
		publishWatermark: pw,
	}

	return pf
}

// buildPublisherMap builds publisher for each toBuffer
func buildPublisherMap(toBuffers map[string]isb.BufferWriter) map[string]publish.Publisher {
	var ctx = context.Background()

	var publisherHBKeyspace = "publisherTest_PROCESSORS"

	var publisherOTKeyspace = "publisherTest_OT_publisherTestPod1"

	heartbeatKV, _, _ := inmem.NewKVInMemKVStore(ctx, "testPublisher", publisherHBKeyspace)
	otKV, _, _ := inmem.NewKVInMemKVStore(ctx, "testPublisher", publisherOTKeyspace)

	publishers := make(map[string]publish.Publisher)
	for key := range toBuffers {
		publishEntity := processor.NewProcessorEntity(key)
		p := publish.NewPublish(ctx, publishEntity, wmstore.BuildWatermarkStore(heartbeatKV, otKV), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	return publishers
}

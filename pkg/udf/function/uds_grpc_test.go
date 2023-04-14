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

package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/numaproj/numaflow-go/pkg/function/clienttest"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMockUDSGRPCBasedUDF(mockClient *funcmock.MockUserDefinedFunctionClient) *UDSgRPCBasedUDF {
	c, _ := clienttest.New(mockClient)
	return &UDSgRPCBasedUDF{c}
}

func TestGRPCBasedUDF_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&functionpb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedUDF(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

type rpcMsg struct {
	msg proto.Message
}

func (r *rpcMsg) Matches(msg interface{}) bool {
	m, ok := msg.(proto.Message)
	if !ok {
		return false
	}
	return proto.Equal(m, r.msg)
}

func (r *rpcMsg) String() string {
	return fmt.Sprintf("is %s", r.msg)
}

func TestGRPCBasedUDF_BasicApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			Metadata: &functionpb.Metadata{
				Id:           "test_id",
				NumDelivered: 1,
			},
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"test_success_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedUDF(mockClient)
		got, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169600, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_success_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
			Metadata: isb.MessageMetadata{
				NumDelivered: 1,
			},
		},
		)
		assert.NoError(t, err)
		assert.Equal(t, req.Keys, got[0].Keys)
		assert.Equal(t, req.Value, got[0].Payload)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			Metadata: &functionpb.Metadata{
				Id: "test_id",
			},
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, fmt.Errorf("mock error"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedUDF(mockClient)
		_, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})
}

func TestHGRPCBasedUDF_ApplyWithMockClient(t *testing.T) {
	multiplyBy2 := func(body []byte) interface{} {
		var result testutils.PayloadForTest
		_ = json.Unmarshal(body, &result)
		result.Value = result.Value * 2
		return result
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().MapFn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, datum *functionpb.DatumRequest, opts ...grpc.CallOption) (*functionpb.DatumResponseList, error) {
			var originalValue testutils.PayloadForTest
			_ = json.Unmarshal(datum.GetValue(), &originalValue)
			doubledValue, _ := json.Marshal(multiplyBy2(datum.GetValue()).(testutils.PayloadForTest))
			var elements []*functionpb.DatumResponse
			if originalValue.Value%2 == 0 {
				elements = append(elements, &functionpb.DatumResponse{
					Keys:  []string{"even"},
					Value: doubledValue,
				})
			} else {
				elements = append(elements, &functionpb.DatumResponse{
					Keys:  []string{"odd"},
					Value: doubledValue,
				})
			}
			datumList := &functionpb.DatumResponseList{
				Elements: elements,
			}
			return datumList, nil
		},
	).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedUDF(mockClient)

	var count = int64(10)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1661169600, 0))

	var results = make([][]byte, len(readMessages))
	var resultKeys = make([][]string, len(readMessages))
	for idx, readMessage := range readMessages {
		apply, err := u.ApplyMap(ctx, &readMessage)
		assert.NoError(t, err)
		results[idx] = apply[0].Payload
		resultKeys[idx] = apply[0].Header.Keys
	}

	var expectedResults = make([][]byte, count)
	var expectedKeys = make([][]string, count)
	for idx, readMessage := range readMessages {
		var readMessagePayload testutils.PayloadForTest
		_ = json.Unmarshal(readMessage.Payload, &readMessagePayload)
		if readMessagePayload.Value%2 == 0 {
			expectedKeys[idx] = []string{"even"}
		} else {
			expectedKeys[idx] = []string{"odd"}
		}
		marshal, _ := json.Marshal(multiplyBy2(readMessage.Payload))
		expectedResults[idx] = marshal
	}

	assert.Equal(t, expectedResults, results)
	assert.Equal(t, expectedKeys, resultKeys)
}

func TestGRPCBasedUDF_BasicReduceWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		mockReduceClient := funcmock.NewMockUserDefinedFunction_ReduceFnClient(ctrl)

		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"reduced_result_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil).Times(1)
		mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"reduced_result_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, io.EOF).Times(1)

		messageCh := make(chan *isb.ReadMessage)

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedUDF(mockClient)
		messages := testutils.BuildTestReadMessages(10, time.Now())

		go func() {
			for index := range messages {
				messageCh <- &messages[index]
			}
			close(messageCh)
		}()

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}
		got, err := u.ApplyReduce(ctx, partitionID, messageCh)

		assert.Len(t, got, 1)
		assert.Equal(t, time.Unix(120, 0).Add(-1*time.Millisecond), got[0].EventTime)
		assert.NoError(t, err)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		mockReduceClient := funcmock.NewMockUserDefinedFunction_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"reduced_result_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, errors.New("mock error for reduce")).AnyTimes()

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedUDF(mockClient)

		messageCh := make(chan *isb.ReadMessage)
		messages := testutils.BuildTestReadMessages(10, time.Now())

		go func() {
			for index := range messages {
				messageCh <- &messages[index]
			}
			close(messageCh)
		}()

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}

		_, err := u.ApplyReduce(ctx, partitionID, messageCh)

		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test context close", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		mockReduceClient := funcmock.NewMockUserDefinedFunction_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"reduced_result_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil).Times(1)

		mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"reduced_result_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, io.EOF).Times(1)

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		u := NewMockUDSGRPCBasedUDF(mockClient)

		messageCh := make(chan *isb.ReadMessage)

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}
		_, err := u.ApplyReduce(ctx, partitionID, messageCh)

		assert.Error(t, err, ctx.Err())
	})
}

func TestHGRPCBasedUDF_Reduce(t *testing.T) {
	sumFunc := func(dataStreamCh <-chan *functionpb.DatumRequest) interface{} {
		var sum testutils.PayloadForTest
		for datum := range dataStreamCh {
			var payLoad testutils.PayloadForTest
			_ = json.Unmarshal(datum.GetValue(), &payLoad)
			sum.Value += payLoad.Value
		}
		return sum
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	messageCh := make(chan *isb.ReadMessage, 10)
	datumStreamCh := make(chan *functionpb.DatumRequest, 10)
	messages := testutils.BuildTestReadMessages(10, time.Now())

	go func() {
		for index := range messages {
			messageCh <- &messages[index]
			datumStreamCh <- createDatum(&messages[index])
		}
		close(messageCh)
		close(datumStreamCh)
	}()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockReduceClient := funcmock.NewMockUserDefinedFunction_ReduceFnClient(ctrl)
	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockReduceClient.EXPECT().Recv().DoAndReturn(
		func() (*functionpb.DatumResponseList, error) {
			result := sumFunc(datumStreamCh)
			sumValue, _ := json.Marshal(result.(testutils.PayloadForTest))
			var elements []*functionpb.DatumResponse
			elements = append(elements, &functionpb.DatumResponse{
				Keys:  []string{"sum"},
				Value: sumValue,
			})
			datumList := &functionpb.DatumResponseList{
				Elements: elements,
			}
			return datumList, nil
		}).Times(1)
	mockReduceClient.EXPECT().Recv().Return(&functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, io.EOF).Times(1)

	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedUDF(mockClient)

	partitionID := &partition.ID{
		Start: time.Unix(60, 0),
		End:   time.Unix(120, 0),
		Slot:  "test",
	}

	result, err := u.ApplyReduce(ctx, partitionID, messageCh)

	var resultPayload testutils.PayloadForTest
	_ = json.Unmarshal(result[0].Payload, &resultPayload)

	assert.NoError(t, err)
	assert.Equal(t, []string{"sum"}, result[0].Keys)
	assert.Equal(t, int64(45), resultPayload.Value)
	assert.Equal(t, time.Unix(120, 0).Add(-1*time.Millisecond), result[0].EventTime)
}

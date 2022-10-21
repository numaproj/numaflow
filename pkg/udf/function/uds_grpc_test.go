package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/numaproj/numaflow-go/pkg/function/clienttest"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMockUDSGRPCBasedUDF(mockClient *funcmock.MockUserDefinedFunctionClient) *udsGRPCBasedUDF {
	c, _ := clienttest.New(mockClient)
	return &udsGRPCBasedUDF{c}
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
		req := &functionpb.Datum{
			Key:       "test_success_key",
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(&functionpb.DatumList{
			Elements: []*functionpb.Datum{
				{
					Key:   "test_success_key",
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
		got, err := u.Apply(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: time.Unix(1661169600, 0),
					},
					ID:  "test_id",
					Key: `test_success_key`,
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.NoError(t, err)
		assert.Equal(t, req.Key, string(got[0].Key))
		assert.Equal(t, req.Value, got[0].Payload)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.Datum{
			Key:       "test_error_key",
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
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
		_, err := u.Apply(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:  "test_id",
					Key: `test_error_key`,
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
		func(_ context.Context, datum *functionpb.Datum, opts ...grpc.CallOption) (*functionpb.DatumList, error) {
			var originalValue testutils.PayloadForTest
			_ = json.Unmarshal(datum.GetValue(), &originalValue)
			doubledValue, _ := json.Marshal(multiplyBy2(datum.GetValue()).(testutils.PayloadForTest))
			var elements []*functionpb.Datum
			if originalValue.Value%2 == 0 {
				elements = append(elements, &functionpb.Datum{
					Key:   "even",
					Value: doubledValue,
				})
			} else {
				elements = append(elements, &functionpb.Datum{
					Key:   "odd",
					Value: doubledValue,
				})
			}
			datumList := &functionpb.DatumList{
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
	var resultKeys = make([]string, len(readMessages))
	for idx, readMessage := range readMessages {
		apply, err := u.Apply(ctx, &readMessage)
		assert.NoError(t, err)
		results[idx] = apply[0].Payload
		resultKeys[idx] = string(apply[0].Header.Key)
	}

	var expectedResults = make([][]byte, count)
	var expectedKeys = make([]string, count)
	for idx, readMessage := range readMessages {
		var readMessagePayload testutils.PayloadForTest
		_ = json.Unmarshal(readMessage.Payload, &readMessagePayload)
		if readMessagePayload.Value%2 == 0 {
			expectedKeys[idx] = "even"
		} else {
			expectedKeys[idx] = "odd"
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
		mockReduceClient := NewMockUserDefinedFunction_ReduceFnClient(ctrl)

		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseAndRecv().Return(&functionpb.DatumList{
			Elements: []*functionpb.Datum{
				{
					Key:   "reduced_result_key",
					Value: []byte(`forward_message`),
				},
			},
		}, nil)

		messageCh := make(chan *isb.ReadMessage)

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
			Key:   "test",
		}

		got, err := u.Reduce(ctx, partitionID, messageCh)

		assert.Len(t, got, 1)
		assert.NoError(t, err)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		mockReduceClient := NewMockUserDefinedFunction_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseAndRecv().Return(&functionpb.DatumList{
			Elements: []*functionpb.Datum{
				{
					Key:   "reduced_result_key",
					Value: []byte(`forward_message`),
				},
			},
		}, errors.New("mock error for reduce"))

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
			Key:   "test",
		}

		_, err := u.Reduce(ctx, partitionID, messageCh)

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
		mockReduceClient := NewMockUserDefinedFunction_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseAndRecv().Return(&functionpb.DatumList{
			Elements: []*functionpb.Datum{
				{
					Key:   "reduced_result_key",
					Value: []byte(`forward_message`),
				},
			},
		}, nil).AnyTimes()

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		u := NewMockUDSGRPCBasedUDF(mockClient)

		messageCh := make(chan *isb.ReadMessage)

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Key:   "test",
		}
		_, err := u.Reduce(ctx, partitionID, messageCh)

		assert.Error(t, err, ctx.Err())
	})
}

func TestHGRPCBasedUDF_Reduce(t *testing.T) {
	sumFunc := func(dataStreamCh <-chan *functionpb.Datum) interface{} {
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
	datumStreamCh := make(chan *functionpb.Datum, 10)
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
	mockReduceClient := NewMockUserDefinedFunction_ReduceFnClient(ctrl)
	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseAndRecv().DoAndReturn(
		func() (*functionpb.DatumList, error) {
			result := sumFunc(datumStreamCh)
			sumValue, _ := json.Marshal(result.(testutils.PayloadForTest))
			var elements []*functionpb.Datum
			elements = append(elements, &functionpb.Datum{
				Key:   "sum",
				Value: sumValue,
			})
			datumList := &functionpb.DatumList{
				Elements: elements,
			}
			return datumList, nil
		}).AnyTimes()

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
		Key:   "test",
	}

	result, err := u.Reduce(ctx, partitionID, messageCh)

	var resultPayload testutils.PayloadForTest
	_ = json.Unmarshal(result[0].Payload, &resultPayload)

	assert.NoError(t, err)
	assert.Equal(t, result[0].Key, "sum")
	assert.Equal(t, resultPayload.Value, int64(45))
}

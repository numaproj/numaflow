package applier

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/numaproj/numaflow-go/pkg/function/clienttest"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMockUDSGRPCBasedUDF(mockClient *funcmock.MockUserDefinedFunctionClient) *UDSGRPCBasedUDF {
	c, _ := clienttest.New(mockClient)
	return &UDSGRPCBasedUDF{c}
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
			Key:            "test_success_key",
			Value:          []byte(`forward_message`),
			EventTime:      &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			IntervalWindow: &functionpb.IntervalWindow{StartTime: timestamppb.New(time.Unix(1661169600, 0)), EndTime: timestamppb.New(time.Unix(1661169660, 0))},
			PaneInfo:       &functionpb.PaneInfo{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().DoFn(gomock.Any(), &rpcMsg{msg: req}).Return(&functionpb.DatumList{
			Elements: []*functionpb.Datum{
				req,
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
						StartTime: time.Unix(1661169600, 0),
						EndTime:   time.Unix(1661169660, 0),
					},
					ID:  "test_id",
					Key: []byte(`test_success_key`),
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleOffset(func() string { return "0" }),
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
			Key:            "test_error_key",
			Value:          []byte(`forward_message`),
			EventTime:      &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			IntervalWindow: &functionpb.IntervalWindow{StartTime: timestamppb.New(time.Unix(1661169600, 0)), EndTime: timestamppb.New(time.Unix(1661169660, 0))},
			PaneInfo:       &functionpb.PaneInfo{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().DoFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, fmt.Errorf("mock error"))

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
						EventTime: time.Unix(1661169600, 0),
						StartTime: time.Unix(1661169600, 0),
						EndTime:   time.Unix(1661169660, 0),
					},
					ID:  "test_id",
					Key: []byte(`test_error_key`),
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleOffset(func() string { return "0" }),
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
	mockClient.EXPECT().DoFn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, datum *functionpb.Datum, opts ...grpc.CallOption) (*functionpb.DatumList, error) {
			var originalValue testutils.PayloadForTest
			_ = json.Unmarshal(datum.GetValue(), &originalValue)
			doubledValue, _ := json.Marshal(multiplyBy2(datum.GetValue()).(testutils.PayloadForTest))
			var elements []*functionpb.Datum
			if originalValue.Value%2 == 0 {
				elements = append(elements, &functionpb.Datum{
					Key:            "even",
					Value:          doubledValue,
					EventTime:      datum.GetEventTime(),
					PaneInfo:       datum.GetPaneInfo(),
					IntervalWindow: datum.GetIntervalWindow(),
				})
			} else {
				elements = append(elements, &functionpb.Datum{
					Key:            "odd",
					Value:          doubledValue,
					EventTime:      datum.GetEventTime(),
					PaneInfo:       datum.GetPaneInfo(),
					IntervalWindow: datum.GetIntervalWindow(),
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

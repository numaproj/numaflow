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

package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/numaproj/numaflow/pkg/sdkclient/udf/clienttest"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/udf/function"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMockGRPCBasedTransformer(mockClient *funcmock.MockUserDefinedFunctionClient) *GRPCBasedTransformer {
	c, _ := clienttest.New(mockClient)
	return &GRPCBasedTransformer{c}
}

func TestGRPCBasedTransformer_WaitUntilReadyWithMockClient(t *testing.T) {
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

	u := NewMockGRPCBasedTransformer(mockClient)
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

func TestGRPCBasedTransformer_BasicApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(&functionpb.DatumResponseList{
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

		u := NewMockGRPCBasedTransformer(mockClient)
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
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, fmt.Errorf("mock error"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockGRPCBasedTransformer(mockClient)
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
		assert.ErrorIs(t, err, function.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: function.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test error retryable: failed after 5 retries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockGRPCBasedTransformer(mockClient)
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
		assert.ErrorIs(t, err, function.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: function.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test error retryable: failed after 1 retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.InvalidArgument, "mock test err: non retryable").Err())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockGRPCBasedTransformer(mockClient)
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
		assert.ErrorIs(t, err, function.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: function.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test error retryable: success after 1 retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169720, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(&functionpb.DatumResponseList{
			Elements: []*functionpb.DatumResponse{
				{
					Keys:  []string{"test_success_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockGRPCBasedTransformer(mockClient)
		got, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169720, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_success_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.NoError(t, err)
		assert.Equal(t, req.Keys, got[0].Keys)
		assert.Equal(t, req.Value, got[0].Payload)
	})

	t.Run("test error non retryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
		req := &functionpb.DatumRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
			Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		}
		mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.InvalidArgument, "mock test err: non retryable").Err())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockGRPCBasedTransformer(mockClient)
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
		assert.ErrorIs(t, err, function.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: function.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})
}

func TestGRPCBasedTransformer_ApplyWithMockClient_ChangePayload(t *testing.T) {
	multiplyBy2 := func(body []byte) interface{} {
		var result testutils.PayloadForTest
		_ = json.Unmarshal(body, &result)
		result.Value = result.Value * 2
		return result
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().MapTFn(gomock.Any(), gomock.Any()).DoAndReturn(
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

	u := NewMockGRPCBasedTransformer(mockClient)

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

func TestGRPCBasedTransformer_ApplyWithMockClient_ChangeEventTime(t *testing.T) {
	testEventTime := time.Date(1992, 2, 8, 0, 0, 0, 100, time.UTC)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().MapTFn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, datum *functionpb.DatumRequest, opts ...grpc.CallOption) (*functionpb.DatumResponseList, error) {
			var elements []*functionpb.DatumResponse
			elements = append(elements, &functionpb.DatumResponse{
				Keys:      []string{"even"},
				Value:     datum.Value,
				EventTime: &functionpb.EventTime{EventTime: timestamppb.New(testEventTime)},
			})
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

	u := NewMockGRPCBasedTransformer(mockClient)

	var count = int64(2)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1661169600, 0))
	for _, readMessage := range readMessages {
		apply, err := u.ApplyMap(ctx, &readMessage)
		assert.NoError(t, err)
		assert.Equal(t, testEventTime, apply[0].EventTime)
	}
}

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

package udsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1/sourcemock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	sourceclient "github.com/numaproj/numaflow/pkg/sdkclient/source/client"
	"github.com/numaproj/numaflow/pkg/sources/udsource/utils"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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

func NewMockUDSgRPCBasedUDSource(mockClient *sourcemock.MockSourceClient) *GRPCBasedUDSource {
	c, _ := sourceclient.NewFromClient(mockClient)
	return &GRPCBasedUDSource{c}
}

func Test_gRPCBasedUDSource_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sourcepb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSgRPCBasedUDSource(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func Test_gRPCBasedUDSource_ApplyPendingWithMockClient(t *testing.T) {
	t.Run("test success - positive pending count", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testResponse := &sourcepb.PendingResponse{
			Result: &sourcepb.PendingResponse_Result{
				Count: 123,
			},
		}

		mockSourceClient := sourcemock.NewMockSourceClient(ctrl)
		mockSourceClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(testResponse, nil).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockSourceClient)
		count, err := u.ApplyPendingFn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), count)
	})

	t.Run("test success - pending is not available - negative count", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testResponse := &sourcepb.PendingResponse{
			Result: &sourcepb.PendingResponse_Result{
				Count: -123,
			},
		}

		mockSourceClient := sourcemock.NewMockSourceClient(ctrl)
		mockSourceClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(testResponse, nil).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockSourceClient)
		count, err := u.ApplyPendingFn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, isb.PendingNotAvailable, count)
	})

	t.Run("test err", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testResponse := &sourcepb.PendingResponse{
			Result: &sourcepb.PendingResponse_Result{
				Count: 123,
			},
		}

		mockSourceErrClient := sourcemock.NewMockSourceClient(ctrl)
		mockSourceErrClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(testResponse, fmt.Errorf("mock udsource pending error")).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockSourceErrClient)
		count, err := u.ApplyPendingFn(ctx)

		assert.Equal(t, isb.PendingNotAvailable, count)
		assert.Equal(t, fmt.Errorf("mock udsource pending error"), err)
	})
}

func Test_gRPCBasedUDSource_ApplyReadWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockClient := sourcemock.NewMockSourceClient(ctrl)
		mockReadClient := sourcemock.NewMockSource_ReadFnClient(ctrl)

		req := &sourcepb.ReadRequest{
			Request: &sourcepb.ReadRequest_Request{
				NumRecords:  1,
				TimeoutInMs: 1000,
			},
		}

		var TestEventTime = time.Unix(1661169600, 0).UTC()
		expectedResponse := &sourcepb.ReadResponse{
			Result: &sourcepb.ReadResponse_Result{
				Payload:   []byte(`test_payload`),
				Offset:    &sourcepb.Offset{Offset: []byte(`test_offset`), PartitionId: "0"},
				EventTime: timestamppb.New(TestEventTime),
				Keys:      []string{"test_key"},
			},
		}

		mockReadClient.EXPECT().Recv().Return(expectedResponse, nil).Times(1)
		mockReadClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)
		mockClient.EXPECT().ReadFn(gomock.Any(), &rpcMsg{msg: req}).Return(mockReadClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockClient)
		readMessages, err := u.ApplyReadFn(ctx, 1, time.Millisecond*1000)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(readMessages))
		assert.Equal(t, []byte(`test_payload`), readMessages[0].Body.Payload)
		assert.Equal(t, []string{"test_key"}, readMessages[0].Keys)
		assert.Equal(t, utils.NewSimpleSourceOffset("test_offset", 0), readMessages[0].ReadOffset)
		assert.Equal(t, TestEventTime, readMessages[0].EventTime)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := sourcemock.NewMockSourceClient(ctrl)
		mockReadClient := sourcemock.NewMockSource_ReadFnClient(ctrl)

		req := &sourcepb.ReadRequest{
			Request: &sourcepb.ReadRequest_Request{
				NumRecords:  1,
				TimeoutInMs: 1000,
			},
		}

		var TestEventTime = time.Unix(1661169600, 0).UTC()
		expectedResponse := &sourcepb.ReadResponse{
			Result: &sourcepb.ReadResponse_Result{
				Payload:   []byte(`test_payload`),
				Offset:    &sourcepb.Offset{Offset: []byte(`test_offset`), PartitionId: "0"},
				EventTime: timestamppb.New(TestEventTime),
				Keys:      []string{"test_key"},
			},
		}

		mockReadClient.EXPECT().Recv().Return(expectedResponse, errors.New("mock error for read")).AnyTimes()
		mockClient.EXPECT().ReadFn(gomock.Any(), &rpcMsg{msg: req}).Return(mockReadClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockClient)
		readMessages, err := u.ApplyReadFn(ctx, 1, time.Millisecond*1000)
		assert.Error(t, err)
		assert.Equal(t, 0, len(readMessages))
	})
}

func Test_gRPCBasedUDSource_ApplyAckWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := sourcemock.NewMockSourceClient(ctrl)
		req := &sourcepb.AckRequest{
			Request: &sourcepb.AckRequest_Request{
				Offsets: []*sourcepb.Offset{
					{Offset: []byte("test-offset-1"), PartitionId: "0"}, {Offset: []byte("test-offset-2"), PartitionId: "0"},
				},
			},
		}

		mockClient.EXPECT().AckFn(gomock.Any(), &rpcMsg{msg: req}).Return(&sourcepb.AckResponse{Result: &sourcepb.AckResponse_Result{Success: &emptypb.Empty{}}}, nil).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockClient)
		err := u.ApplyAckFn(ctx, []isb.Offset{
			utils.NewSimpleSourceOffset("test-offset-1", 0),
			utils.NewSimpleSourceOffset("test-offset-2", 0),
		})
		assert.NoError(t, err)
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := sourcemock.NewMockSourceClient(ctrl)
		req := &sourcepb.AckRequest{
			Request: &sourcepb.AckRequest_Request{
				Offsets: []*sourcepb.Offset{
					{Offset: []byte("test-offset-1"), PartitionId: "0"}, {Offset: []byte("test-offset-2"), PartitionId: "0"},
				},
			},
		}

		mockClient.EXPECT().AckFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockClient)
		err := u.ApplyAckFn(ctx, []isb.Offset{
			utils.NewSimpleSourceOffset("test-offset-1", 0),
			utils.NewSimpleSourceOffset("test-offset-2", 0),
		})
		assert.ErrorIs(t, err, status.New(codes.DeadlineExceeded, "mock test err").Err())
	})
}

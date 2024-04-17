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

package udsink

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1/sinkmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	sinkclient "github.com/numaproj/numaflow/pkg/sdkclient/sinker"
)

func NewMockUDSgRPCBasedUDSink(mockClient *sinkmock.MockSinkClient) *UDSgRPCBasedUDSink {
	c, _ := sinkclient.NewFromClient(mockClient)
	return &UDSgRPCBasedUDSink{c}
}

func Test_gRPCBasedUDSink_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sinkmock.NewMockSinkClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSgRPCBasedUDSink(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func Test_gRPCBasedUDSink_ApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testDatumList := []*sinkpb.SinkRequest{
			{
				Id:        "test_id_0",
				Value:     []byte(`sink_message_success`),
				EventTime: timestamppb.New(time.Unix(1661169660, 0)),
				Watermark: timestamppb.New(time.Time{}),
			},
			{
				Id:        "test_id_1",
				Value:     []byte(`sink_message_err`),
				EventTime: timestamppb.New(time.Unix(1661169660, 0)),
				Watermark: timestamppb.New(time.Time{}),
			},
		}
		testResponseList := []*sinkpb.SinkResponse_Result{
			{
				Id:     "test_id_0",
				Status: sinkpb.Status_SUCCESS,
				ErrMsg: "",
			},
			{
				Id:     "test_id_1",
				Status: sinkpb.Status_FAILURE,
				ErrMsg: "mock sink message error",
			},
		}

		mockSinkClient := sinkmock.NewMockSink_SinkFnClient(ctrl)
		mockSinkClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockSinkClient.EXPECT().CloseAndRecv().Return(&sinkpb.SinkResponse{
			Results: testResponseList,
		}, nil)

		mockClient := sinkmock.NewMockSinkClient(ctrl)
		mockClient.EXPECT().SinkFn(gomock.Any(), gomock.Any()).Return(mockSinkClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSink(mockClient)
		gotErrList := u.ApplySink(ctx, testDatumList)
		assert.Equal(t, 2, len(gotErrList))
		assert.Equal(t, nil, gotErrList[0])

		assert.Equal(t, ApplyUDSinkErr{
			UserUDSinkErr: true,
			Message:       "mock sink message error",
		}, gotErrList[1])
	})

	t.Run("test err", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testDatumList := []*sinkpb.SinkRequest{
			{
				Id:        "test_id_0",
				Value:     []byte(`sink_message_grpc_err`),
				EventTime: timestamppb.New(time.Unix(1661169660, 0)),
				Watermark: timestamppb.New(time.Time{}),
			},
			{
				Id:        "test_id_1",
				Value:     []byte(`sink_message_grpc_err`),
				EventTime: timestamppb.New(time.Unix(1661169660, 0)),
				Watermark: timestamppb.New(time.Time{}),
			},
		}

		mockSinkErrClient := sinkmock.NewMockSink_SinkFnClient(ctrl)
		mockSinkErrClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()

		mockClient := sinkmock.NewMockSinkClient(ctrl)
		mockClient.EXPECT().SinkFn(gomock.Any(), gomock.Any()).Return(mockSinkErrClient, fmt.Errorf("mock SinkFn error"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSink(mockClient)
		gotErrList := u.ApplySink(ctx, testDatumList)
		expectedErrList := []error{
			ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       "gRPC client.SinkFn failed, failed to execute c.grpcClt.SinkFn(): mock SinkFn error",
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			},
			ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       "gRPC client.SinkFn failed, failed to execute c.grpcClt.SinkFn(): mock SinkFn error",
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			},
		}
		assert.Equal(t, 2, len(gotErrList))
		assert.Equal(t, expectedErrList, gotErrList)
	})
}

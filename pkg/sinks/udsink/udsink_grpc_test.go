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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1/sinkmock"
	"github.com/numaproj/numaflow-go/pkg/sink/clienttest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMockUDSGRPCBasedUDF(mockClient *sinkmock.MockUserDefinedSinkClient) *udsGRPCBasedUDSink {
	c, _ := clienttest.New(mockClient)
	return &udsGRPCBasedUDSink{c}
}

func TestGRPCBasedUDF_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sinkmock.NewMockUserDefinedSinkClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: true}, nil)

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

func TestGRPCBasedUDF_ApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testDatumList := []*sinkpb.Datum{
			{
				Id:        "test_id_0",
				Value:     []byte(`sink_message_success`),
				EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
				Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			},
			{
				Id:        "test_id_1",
				Value:     []byte(`sink_message_err`),
				EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
				Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			},
		}
		testResponseList := []*sinkpb.Response{
			{
				Id:      "test_id_0",
				Success: true,
				ErrMsg:  "",
			},
			{
				Id:      "test_id_1",
				Success: false,
				ErrMsg:  "mock sink message error",
			},
		}

		mockClient := sinkmock.NewMockUserDefinedSinkClient(ctrl)

		mockClient.EXPECT().SinkFn(gomock.Any(), &rpcMsg{msg: &sinkpb.DatumList{
			Elements: testDatumList,
		}}).Return(&sinkpb.ResponseList{
			Responses: testResponseList,
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
		gotErrList := u.Apply(ctx, testDatumList)
		assert.Equal(t, 2, len(gotErrList))
		assert.Equal(t, nil, gotErrList[0])
		assert.Equal(t, fmt.Errorf("mock sink message error"), gotErrList[1])
	})

	t.Run("test err", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testDatumList := []*sinkpb.Datum{
			{
				Id:        "test_id_0",
				Value:     []byte(`sink_message_grpc_err`),
				EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
				Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			},
			{
				Id:        "test_id_1",
				Value:     []byte(`sink_message_grpc_err`),
				EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169660, 0))},
				Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})},
			},
		}

		mockClient := sinkmock.NewMockUserDefinedSinkClient(ctrl)

		mockClient.EXPECT().SinkFn(gomock.Any(), &rpcMsg{msg: &sinkpb.DatumList{
			Elements: testDatumList,
		}}).Return(&sinkpb.ResponseList{
			Responses: nil,
		}, fmt.Errorf("mock error"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedUDF(mockClient)
		gotErrList := u.Apply(ctx, testDatumList)
		expectedErrList := []error{
			ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       "gRPC client.SinkFn failed, failed to execute c.grpcClt.SinkFn(): mock error",
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			},
			ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       "gRPC client.SinkFn failed, failed to execute c.grpcClt.SinkFn(): mock error",
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

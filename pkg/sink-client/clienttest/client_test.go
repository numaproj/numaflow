package clienttest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1/sinkmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func TestIsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sinkmock.NewMockUserDefinedSinkClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	ready, err := testClient.IsReady(ctx, &emptypb.Empty{})
	assert.True(t, ready)
	assert.NoError(t, err)

	ready, err = testClient.IsReady(ctx, &emptypb.Empty{})
	assert.False(t, ready)
	assert.EqualError(t, err, "mock connection refused")
}

func TestSinkFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSinkClient := sinkmock.NewMockUserDefinedSink_SinkFnClient(ctrl)
	mockSinkClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockSinkClient.EXPECT().CloseAndRecv().Return(&sinkpb.ResponseList{
		Responses: []*sinkpb.Response{
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
		},
	}, nil)

	mockSinkErrClient := sinkmock.NewMockUserDefinedSink_SinkFnClient(ctrl)
	mockSinkErrClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()

	mockClient := sinkmock.NewMockUserDefinedSinkClient(ctrl)
	testDatumList := []*sinkpb.DatumRequest{
		{
			Id:        "test_id_0",
			Value:     []byte(`sink_message_success`),
			EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
			Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})},
		},
		{
			Id:        "test_id_1",
			Value:     []byte(`sink_message_err`),
			EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
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
	mockClient.EXPECT().SinkFn(gomock.Any(), gomock.Any()).Return(mockSinkClient, nil)
	mockClient.EXPECT().SinkFn(gomock.Any(), gomock.Any()).Return(mockSinkErrClient, fmt.Errorf("mock SinkFn error"))

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	got, err := testClient.SinkFn(ctx, testDatumList)
	reflect.DeepEqual(got, testResponseList)
	assert.NoError(t, err)

	got, err = testClient.SinkFn(ctx, testDatumList)
	assert.Nil(t, got)
	assert.EqualError(t, err, "failed to execute c.grpcClt.SinkFn(): mock SinkFn error")
}

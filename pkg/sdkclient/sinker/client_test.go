package sinker

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1/sinkmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sinkmock.NewMockSinkClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sinkpb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
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

func TestClient_SinkFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSinkClient := sinkmock.NewMockSink_SinkFnClient(ctrl)
	mockSinkClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockSinkClient.EXPECT().CloseAndRecv().Return(&sinkpb.SinkResponse{
		Results: []*sinkpb.SinkResponse_Result{
			{
				Id:      "temp-id",
				Success: true,
			},
		},
	}, nil)

	mockClient := sinkmock.NewMockSinkClient(ctrl)
	mockClient.EXPECT().SinkFn(gomock.Any(), gomock.Any()).Return(mockSinkClient, nil)

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	response, err := testClient.SinkFn(ctx, []*sinkpb.SinkRequest{})
	assert.Equal(t, &sinkpb.SinkResponse{Results: []*sinkpb.SinkResponse_Result{
		{
			Id:      "temp-id",
			Success: true,
		},
	}}, response)
	assert.NoError(t, err)

}

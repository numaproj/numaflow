package mapstreamer

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mapstreampb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1/mapstreammock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mapstreampb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mapstreampb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestClient_MapStreamFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
	mockStreamClient := mapstreammock.NewMockMapStream_MapStreamFnClient(ctrl)

	mockStreamClient.EXPECT().Recv().Return(&mapstreampb.MapStreamResponse{Result: &mapstreampb.MapStreamResponse_Result{
		Keys:  []string{"temp-key"},
		Value: []byte("mock result"),
		Tags:  nil,
	}}, nil)

	mockStreamClient.EXPECT().Recv().Return(&mapstreampb.MapStreamResponse{Result: &mapstreampb.MapStreamResponse_Result{
		Keys:  []string{"temp-key"},
		Value: []byte("mock result"),
		Tags:  nil,
	}}, io.EOF)

	mockStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()

	mockClient.EXPECT().MapStreamFn(gomock.Any(), gomock.Any()).Return(mockStreamClient, nil)

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	responseCh := make(chan *mapstreampb.MapStreamResponse)

	go func() {
		select {
		case <-ctx.Done():
			return
		case resp := <-responseCh:
			assert.Equal(t, resp, &mapstreampb.MapStreamResponse{Result: &mapstreampb.MapStreamResponse_Result{
				Keys:  []string{"temp-key"},
				Value: []byte("mock result"),
				Tags:  nil,
			}})
		}
	}()

	err = testClient.MapStreamFn(ctx, &mapstreampb.MapStreamRequest{}, responseCh)
	assert.NoError(t, err)
}

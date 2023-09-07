package sourcetransformer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	transformpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
	transformermock "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1/transformmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := transformermock.NewMockSourceTransformClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&transformpb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&transformpb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestClient_SourceTransformFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := transformermock.NewMockSourceTransformClient(ctrl)
	mockClient.EXPECT().SourceTransformFn(gomock.Any(), gomock.Any()).Return(&transformpb.SourceTransformResponse{Results: []*transformpb.SourceTransformResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, nil)
	mockClient.EXPECT().SourceTransformFn(gomock.Any(), gomock.Any()).Return(&transformpb.SourceTransformResponse{Results: []*transformpb.SourceTransformResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	result, err := testClient.SourceTransformFn(ctx, &transformpb.SourceTransformRequest{})
	assert.Equal(t, &transformpb.SourceTransformResponse{Results: []*transformpb.SourceTransformResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, result)
	assert.NoError(t, err)

	_, err = testClient.SourceTransformFn(ctx, &transformpb.SourceTransformRequest{})
	assert.EqualError(t, err, "NonRetryable: mock connection refused")
}

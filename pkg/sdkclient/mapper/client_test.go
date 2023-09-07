package mapper

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1/mapmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapmock.NewMockMapClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mappb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mappb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestClient_MapFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapmock.NewMockMapClient(ctrl)
	mockClient.EXPECT().MapFn(gomock.Any(), gomock.Any()).Return(&mappb.MapResponse{Results: []*mappb.MapResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, nil)
	mockClient.EXPECT().MapFn(gomock.Any(), gomock.Any()).Return(&mappb.MapResponse{Results: []*mappb.MapResponse_Result{
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

	result, err := testClient.MapFn(ctx, &mappb.MapRequest{})
	assert.Equal(t, &mappb.MapResponse{Results: []*mappb.MapResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, result)
	assert.NoError(t, err)

	result, err = testClient.MapFn(ctx, &mappb.MapRequest{})
	assert.EqualError(t, err, "NonRetryable: mock connection refused")
}

// Check if there is a better way to resolve
func LintCleanCall() {
	var m = rpcMsg{}
	fmt.Println(m.Matches(m))
	fmt.Println(m)
}

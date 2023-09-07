package reducer

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1/reducemock"
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

	mockClient := reducemock.NewMockReduceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&reducepb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&reducepb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestClient_ReduceFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := reducemock.NewMockReduceClient(ctrl)
	mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Results: []*reducepb.ReduceResponse_Result{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, nil).Times(1)
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Results: []*reducepb.ReduceResponse_Result{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, io.EOF).Times(1)
	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	messageCh := make(chan *reducepb.ReduceRequest)
	close(messageCh)
	response, err := testClient.ReduceFn(ctx, messageCh)
	assert.Equal(t, &reducepb.ReduceResponse{
		Results: []*reducepb.ReduceResponse_Result{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}, response)
	assert.NoError(t, err)

}

// Check if there is a better way to resolve
func LintCleanCall() {
	var m = rpcMsg{}
	fmt.Println(m.Matches(m))
	fmt.Println(m)
}

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
package reducer

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1/reducemock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

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

func TestClient_AsyncReduceFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := reducemock.NewMockReduceClient(ctrl)
	mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Result: &reducepb.ReduceResponse_Result{
			Keys:      []string{"reduced_result_key1"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(120, 0).Add(-1 * time.Millisecond)),
		},
		Partition: &reducepb.Partition{
			Start: timestamppb.New(time.Unix(60, 0)),
			End:   timestamppb.New(time.Unix(120, 0)),
			Slot:  "slot-0",
		},
	}, nil).Times(1)
	mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
		Result: &reducepb.ReduceResponse_Result{
			Keys:      []string{"reduced_result_key2"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(120, 0).Add(-1 * time.Millisecond)),
		},
		Partition: &reducepb.Partition{
			Start: timestamppb.New(time.Unix(60, 0)),
			End:   timestamppb.New(time.Unix(120, 0)),
			Slot:  "slot-0",
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
	responseCh, _ := testClient.AsyncReduceFn(ctx, messageCh)
	for response := range responseCh {
		assert.Equal(t, &reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:      []string{"reduced_result_key1"},
				Value:     []byte(`forward_message`),
				EventTime: timestamppb.New(time.Unix(120, 0).Add(-1 * time.Millisecond)),
			},
			Partition: &reducepb.Partition{
				Start: timestamppb.New(time.Unix(60, 0)),
				End:   timestamppb.New(time.Unix(120, 0)),
				Slot:  "slot-0",
			},
		}, response)
	}
}

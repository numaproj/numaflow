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
package sessionreducer

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sessionreducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/sessionreduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sessionreduce/v1/sessionreducemock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sessionreducemock.NewMockSessionReduceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sessionreducepb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sessionreducepb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestClient_SessionReduceFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sessionreducemock.NewMockSessionReduceClient(ctrl)
	mockReduceClient := sessionreducemock.NewMockSessionReduce_SessionReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockReduceClient.EXPECT().Recv().Return(&sessionreducepb.SessionReduceResponse{
		Result: &sessionreducepb.SessionReduceResponse_Result{
			Keys:  []string{"reduced_result_key1"},
			Value: []byte(`forward_message`),
		},
		KeyedWindow: &sessionreducepb.KeyedWindow{
			Start: timestamppb.New(time.Unix(60, 0)),
			End:   timestamppb.New(time.Unix(120, 0)),
			Slot:  "slot-0",
		},
	}, nil).Times(1)
	mockReduceClient.EXPECT().Recv().Return(&sessionreducepb.SessionReduceResponse{
		Result: &sessionreducepb.SessionReduceResponse_Result{
			Keys:  []string{"reduced_result_key2"},
			Value: []byte(`forward_message`),
		},
		KeyedWindow: &sessionreducepb.KeyedWindow{
			Start: timestamppb.New(time.Unix(60, 0)),
			End:   timestamppb.New(time.Unix(120, 0)),
			Slot:  "slot-0",
		},
	}, io.EOF).Times(1)
	mockClient.EXPECT().SessionReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	messageCh := make(chan *sessionreducepb.SessionReduceRequest)
	close(messageCh)
	responseCh, _ := testClient.SessionReduceFn(ctx, messageCh)
	for response := range responseCh {
		assert.Equal(t, &sessionreducepb.SessionReduceResponse{
			Result: &sessionreducepb.SessionReduceResponse_Result{
				Keys:  []string{"reduced_result_key1"},
				Value: []byte(`forward_message`),
			},
			KeyedWindow: &sessionreducepb.KeyedWindow{
				Start: timestamppb.New(time.Unix(60, 0)),
				End:   timestamppb.New(time.Unix(120, 0)),
				Slot:  "slot-0",
			},
		}, response)
	}
}

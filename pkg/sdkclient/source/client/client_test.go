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

package client

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1/sourcemock"
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
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sourcepb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sourcepb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

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

func TestReadFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockStreamClient := sourcemock.NewMockSource_ReadFnClient(ctrl)

	var TestEventTime = time.Unix(1661169600, 0).UTC()
	expectedResp := &sourcepb.ReadResponse{
		Result: &sourcepb.ReadResponse_Result{
			Payload:   []byte(`test_payload`),
			Offset:    &sourcepb.Offset{Offset: []byte(`test_offset`), PartitionId: "0"},
			EventTime: timestamppb.New(TestEventTime),
			Keys:      []string{"test_key"},
		},
	}

	const numRecords = 5
	for i := 0; i < numRecords; i++ {
		mockStreamClient.EXPECT().Recv().Return(expectedResp, nil)
	}
	mockStreamClient.EXPECT().Recv().Return(expectedResp, io.EOF)

	mockStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockClient.EXPECT().ReadFn(gomock.Any(), gomock.Any()).Return(mockStreamClient, nil)

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	}))

	responseCh := make(chan *sourcepb.ReadResponse)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-responseCh:
				if !ok {
					return
				}
				assert.True(t, reflect.DeepEqual(resp, expectedResp))
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = testClient.ReadFn(ctx, &sourcepb.ReadRequest{
			Request: &sourcepb.ReadRequest_Request{
				NumRecords: uint64(numRecords),
			},
		}, responseCh)
		assert.NoError(t, err)
	}()
	wg.Wait()
	close(responseCh)
}

func TestAckFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockClient.EXPECT().AckFn(gomock.Any(), gomock.Any()).Return(&sourcepb.AckResponse{}, nil)
	mockClient.EXPECT().AckFn(gomock.Any(), gomock.Any()).Return(&sourcepb.AckResponse{}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	ack, err := testClient.AckFn(ctx, &sourcepb.AckRequest{})
	assert.NoError(t, err)
	assert.Equal(t, &sourcepb.AckResponse{}, ack)

	ack, err = testClient.AckFn(ctx, &sourcepb.AckRequest{})
	assert.EqualError(t, err, "mock connection refused")
	assert.Equal(t, &sourcepb.AckResponse{}, ack)
}

func TestPendingFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(&sourcepb.PendingResponse{
		Result: &sourcepb.PendingResponse_Result{
			Count: 123,
		},
	}, nil)
	mockClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(&sourcepb.PendingResponse{}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	pending, err := testClient.PendingFn(ctx, &emptypb.Empty{})
	assert.NoError(t, err)
	reflect.DeepEqual(pending, &sourcepb.PendingResponse{
		Result: &sourcepb.PendingResponse_Result{
			Count: 123,
		},
	})

	pending, err = testClient.PendingFn(ctx, &emptypb.Empty{})
	assert.EqualError(t, err, "mock connection refused")
	assert.Equal(t, &sourcepb.PendingResponse{}, pending)
}

// Check if there is a better way to resolve
func LintCleanCall() {
	var m = rpcMsg{}
	fmt.Println(m.Matches(m))
	fmt.Println(m)
}

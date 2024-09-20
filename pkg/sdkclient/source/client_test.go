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

package source

import (
	"context"
	"fmt"
	"reflect"
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

	testClient := client{grpcClt: mockClient}

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
			Offset:    &sourcepb.Offset{Offset: []byte(`test_offset`), PartitionId: 0},
			EventTime: timestamppb.New(TestEventTime),
			Keys:      []string{"test_key"},
		},
	}

	const numRecords = 5
	for i := 0; i < numRecords; i++ {
		mockStreamClient.EXPECT().Recv().Return(expectedResp, nil)
	}

	eotResponse := &sourcepb.ReadResponse{
		Status: &sourcepb.ReadResponse_Status{
			Eot:  true,
			Code: 0,
		},
	}
	mockStreamClient.EXPECT().Recv().Return(eotResponse, nil)

	mockStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()

	request := &sourcepb.ReadRequest{
		Request: &sourcepb.ReadRequest_Request{
			NumRecords: uint64(numRecords),
		},
	}
	mockStreamClient.EXPECT().Send(request).Return(nil)

	testClient := &client{
		grpcClt:    mockClient,
		readStream: mockStreamClient,
	}

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

	err := testClient.ReadFn(ctx, &sourcepb.ReadRequest{
		Request: &sourcepb.ReadRequest_Request{
			NumRecords: uint64(numRecords),
		},
	}, responseCh)
	assert.NoError(t, err)
	close(responseCh)
}

func TestAckFn(t *testing.T) {
	var ctx = context.Background()
	LintCleanCall()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockStream := sourcemock.NewMockSource_AckFnClient(ctrl)

	// Handshake request and response
	mockStream.EXPECT().Send(&sourcepb.AckRequest{
		Handshake: &sourcepb.Handshake{
			Sot: true,
		},
	}).Return(nil)
	mockStream.EXPECT().Recv().Return(&sourcepb.AckResponse{
		Handshake: &sourcepb.Handshake{
			Sot: true,
		},
	}, nil)

	// Ack request and response
	mockStream.EXPECT().Send(gomock.Any()).Return(nil)
	mockStream.EXPECT().Recv().Return(&sourcepb.AckResponse{}, nil)

	testClient := client{
		grpcClt:   mockClient,
		ackStream: mockStream,
	}

	// Perform handshake
	ackHandshakeRequest := &sourcepb.AckRequest{
		Handshake: &sourcepb.Handshake{
			Sot: true,
		},
	}
	err := testClient.ackStream.Send(ackHandshakeRequest)
	assert.NoError(t, err)

	ackHandshakeResponse, err := testClient.ackStream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, ackHandshakeResponse.GetHandshake())
	assert.True(t, ackHandshakeResponse.GetHandshake().GetSot())

	// Test AckFn
	ack, err := testClient.AckFn(ctx, []*sourcepb.AckRequest{{}})
	assert.NoError(t, err)
	assert.Equal(t, []*sourcepb.AckResponse{{}}, ack)
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

	testClient := client{
		grpcClt: mockClient,
	}

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

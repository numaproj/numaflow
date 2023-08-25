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

package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1/mapmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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

func NewMockUDSGRPCBasedMap(mockClient *mapmock.MockMapClient) *GRPCBasedMap {
	c, _ := mapper.NewFromClient(mockClient)
	return &GRPCBasedMap{c}
}

func TestGRPCBasedMap_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapmock.NewMockMapClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mappb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedMap(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedMap_BasicApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapmock.NewMockMapClient(ctrl)
		req := &mappb.MapRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169600, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(&mappb.MapResponse{
			Results: []*mappb.MapResponse_Result{
				{
					Keys:  []string{"test_success_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMap(mockClient)
		got, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169600, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_success_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
			Metadata: isb.MessageMetadata{
				NumDelivered: 1,
			},
		},
		)
		assert.NoError(t, err)
		assert.Equal(t, req.Keys, got[0].Keys)
		assert.Equal(t, req.Value, got[0].Payload)
	})

	t.Run("test retryable error: failed after 5 retries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapmock.NewMockMapClient(ctrl)
		req := &mappb.MapRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169660, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMap(mockClient)
		_, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test retryable error: failed after 1 retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapmock.NewMockMapClient(ctrl)
		req := &mappb.MapRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169660, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.InvalidArgument, "mock test err: non retryable").Err())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMap(mockClient)
		_, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test retryable error: succeed after 1 retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapmock.NewMockMapClient(ctrl)
		req := &mappb.MapRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169720, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.DeadlineExceeded, "mock test err").Err())
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(&mappb.MapResponse{
			Results: []*mappb.MapResponse_Result{
				{
					Keys:  []string{"test_success_key"},
					Value: []byte(`forward_message`),
				},
			},
		}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMap(mockClient)
		got, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169720, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_success_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
			Metadata: isb.MessageMetadata{
				NumDelivered: 1,
			},
		},
		)
		assert.NoError(t, err)
		assert.Equal(t, req.Keys, got[0].Keys)
		assert.Equal(t, req.Value, got[0].Payload)
	})

	t.Run("test non retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapmock.NewMockMapClient(ctrl)
		req := &mappb.MapRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169660, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: req}).Return(nil, status.New(codes.InvalidArgument, "mock test err: non retryable").Err())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMap(mockClient)
		_, err := u.ApplyMap(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		},
		)
		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})
}

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

//import (
//	"context"
//	"errors"
//	"fmt"
//	"io"
//	"testing"
//	"time"
//
//	"github.com/golang/mock/gomock"
//	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
//	"github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1/reducemock"
//	"github.com/stretchr/testify/assert"
//
//	"github.com/numaproj/numaflow/pkg/isb"
//	"github.com/numaproj/numaflow/pkg/isb/testutils"
//	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
//	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
//)
//
//func NewMockUDSGRPCBasedReduce(mockClient *reducemock.MockReduceClient) *GRPCBasedReduce {
//	c, _ := reducer.NewFromClient(mockClient)
//	return &GRPCBasedReduce{c}
//}
//
//func TestGRPCBasedReduce_WaitUntilReadyWithMockClient(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	mockClient := reducemock.NewMockReduceClient(ctrl)
//	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&reducepb.ReadyResponse{Ready: true}, nil)
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	go func() {
//		<-ctx.Done()
//		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
//			t.Log(t.Name(), "test timeout")
//		}
//	}()
//
//	u := NewMockUDSGRPCBasedReduce(mockClient)
//	err := u.WaitUntilReady(ctx)
//	assert.NoError(t, err)
//}
//
//func TestGRPCBasedUDF_BasicReduceWithMockClient(t *testing.T) {
//	t.Run("test success", func(t *testing.T) {
//
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mockClient := reducemock.NewMockReduceClient(ctrl)
//		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)
//
//		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
//			Results: []*reducepb.ReduceResponse_Result{
//				{
//					Keys:  []string{"reduced_result_key"},
//					Value: []byte(`forward_message`),
//				},
//			},
//		}, nil).Times(1)
//		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
//			Results: []*reducepb.ReduceResponse_Result{
//				{
//					Keys:  []string{"reduced_result_key"},
//					Value: []byte(`forward_message`),
//				},
//			},
//		}, io.EOF).Times(1)
//
//		messageCh := make(chan *isb.ReadMessage)
//
//		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)
//
//		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//		defer cancel()
//		go func() {
//			<-ctx.Done()
//			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
//				t.Log(t.Name(), "test timeout")
//			}
//		}()
//
//		u := NewMockUDSGRPCBasedReduce(mockClient)
//		messages := testutils.BuildTestReadMessages(10, time.Now())
//
//		go func() {
//			for index := range messages {
//				messageCh <- &messages[index]
//			}
//			close(messageCh)
//		}()
//
//		partitionID := &partition.ID{
//			Start: time.Unix(60, 0),
//			End:   time.Unix(120, 0),
//			Slot:  "test",
//		}
//		got, err := u.ApplyReduce(ctx, partitionID, messageCh)
//
//		assert.Len(t, got, 1)
//		assert.Equal(t, time.Unix(120, 0).Add(-1*time.Millisecond), got[0].EventTime)
//		assert.NoError(t, err)
//	})
//
//	t.Run("test error", func(t *testing.T) {
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mockClient := reducemock.NewMockReduceClient(ctrl)
//		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)
//		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
//			Results: []*reducepb.ReduceResponse_Result{
//				{
//					Keys:  []string{"reduced_result_key"},
//					Value: []byte(`forward_message`),
//				},
//			},
//		}, errors.New("mock error for reduce")).AnyTimes()
//
//		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)
//
//		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//		defer cancel()
//
//		go func() {
//			<-ctx.Done()
//			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
//				t.Log(t.Name(), "test timeout")
//			}
//		}()
//
//		u := NewMockUDSGRPCBasedReduce(mockClient)
//
//		messageCh := make(chan *isb.ReadMessage)
//		messages := testutils.BuildTestReadMessages(10, time.Now())
//
//		go func() {
//			for index := range messages {
//				select {
//				case <-ctx.Done():
//					return
//				case messageCh <- &messages[index]:
//				}
//			}
//			close(messageCh)
//		}()
//
//		partitionID := &partition.ID{
//			Start: time.Unix(60, 0),
//			End:   time.Unix(120, 0),
//			Slot:  "test",
//		}
//
//		_, err := u.ApplyReduce(ctx, partitionID, messageCh)
//
//		assert.ErrorIs(t, err, ApplyUDFErr{
//			UserUDFErr: false,
//			Message:    fmt.Sprintf("%s", err),
//			InternalErr: InternalErr{
//				Flag:        true,
//				MainCarDown: false,
//			},
//		})
//	})
//
//	t.Run("test context close", func(t *testing.T) {
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mockClient := reducemock.NewMockReduceClient(ctrl)
//		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)
//		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
//		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
//			Results: []*reducepb.ReduceResponse_Result{
//				{
//					Keys:  []string{"reduced_result_key"},
//					Value: []byte(`forward_message`),
//				},
//			},
//		}, nil).Times(1)
//
//		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
//			Results: []*reducepb.ReduceResponse_Result{
//				{
//					Keys:  []string{"reduced_result_key"},
//					Value: []byte(`forward_message`),
//				},
//			},
//		}, io.EOF).Times(1)
//
//		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)
//
//		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//		defer cancel()
//
//		u := NewMockUDSGRPCBasedReduce(mockClient)
//
//		messageCh := make(chan *isb.ReadMessage)
//
//		partitionID := &partition.ID{
//			Start: time.Unix(60, 0),
//			End:   time.Unix(120, 0),
//			Slot:  "test",
//		}
//		_, err := u.ApplyReduce(ctx, partitionID, messageCh)
//
//		assert.Error(t, err, ctx.Err())
//	})
//}

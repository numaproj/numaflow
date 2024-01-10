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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1/reducemock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	"github.com/numaproj/numaflow/pkg/window"
)

func NewMockUDSGRPCBasedReduce(mockClient *reducemock.MockReduceClient) *GRPCBasedAlignedReduce {
	c, _ := reducer.NewFromClient(mockClient)
	return &GRPCBasedAlignedReduce{c}
}

func TestGRPCBasedReduce_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := reducemock.NewMockReduceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&reducepb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedReduce(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedUDF_AsyncReduceWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := reducemock.NewMockReduceClient(ctrl)
		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)

		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:  []string{"reduced_result_key_1"},
				Value: []byte(`forward_message_1`),
			},
			Window: &reducepb.Window{
				Start: timestamppb.New(time.Unix(60, 0)),
				End:   timestamppb.New(time.Unix(120, 0)),
				Slot:  "test-1",
			},
		}, nil).Times(1)
		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:  []string{"reduced_result_key_2"},
				Value: []byte(`forward_message_2`),
			},
			Window: &reducepb.Window{
				Start: timestamppb.New(time.Unix(60, 0)),
				End:   timestamppb.New(time.Unix(120, 0)),
				Slot:  "test-2",
			},
		}, nil).Times(1)
		mockReduceClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

		requestsCh := make(chan *window.TimedWindowRequest)
		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedReduce(mockClient)
		requests := testutils.BuildTestWindowRequests(10, time.Now(), window.Append)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range requests {
				requestsCh <- &requests[index]
			}
			close(requestsCh)
		}()

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}
		responseCh, _ := u.ApplyReduce(ctx, partitionID, requestsCh)

		for response := range responseCh {
			assert.Equal(t, time.Unix(120, 0).Add(-1*time.Millisecond).UnixMilli(), response.WriteMessage.EventTime.UnixMilli())
		}
		wg.Wait()
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := reducemock.NewMockReduceClient(ctrl)
		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, errors.New("mock error for reduce")).Times(1)
		mockReduceClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedReduce(mockClient)

		requestsCh := make(chan *window.TimedWindowRequest)
		requests := testutils.BuildTestWindowRequests(10, time.Now(), window.Append)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range requests {
				select {
				case <-ctx.Done():
					return
				case requestsCh <- &requests[index]:
				}
			}
			close(requestsCh)
		}()

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}

		responseCh, errCh := u.ApplyReduce(ctx, partitionID, requestsCh)

	readLoop:
		for {
			select {
			case err := <-errCh:
				if err == ctx.Err() {
					assert.Fail(t, "context error")
					break readLoop
				}
				if err != nil {
					assert.ErrorIs(t, err, ApplyUDFErr{
						UserUDFErr: false,
						Message:    err.Error(),
						InternalErr: InternalErr{
							Flag:        true,
							MainCarDown: false,
						},
					})
				}
			case _, ok := <-responseCh:
				if !ok {
					break readLoop
				}
			}

		}
		wg.Wait()
	})

	t.Run("test context close", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := reducemock.NewMockReduceClient(ctrl)
		mockReduceClient := reducemock.NewMockReduce_ReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, nil).Times(1)

		mockReduceClient.EXPECT().Recv().Return(&reducepb.ReduceResponse{
			Result: &reducepb.ReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, io.EOF).Times(1)

		mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		u := NewMockUDSGRPCBasedReduce(mockClient)

		requests := make(chan *window.TimedWindowRequest)

		partitionID := &partition.ID{
			Start: time.Unix(60, 0),
			End:   time.Unix(120, 0),
			Slot:  "test",
		}
		respCh, errCh := u.ApplyReduce(ctx, partitionID, requests)

	readLoop:
		for {
			select {
			case err := <-errCh:
				if err != nil {
					assert.Error(t, err, ctx.Err())
					break readLoop
				}
			case _, ok := <-respCh:
				if !ok {
					break readLoop
				}
			}
		}
	})
}

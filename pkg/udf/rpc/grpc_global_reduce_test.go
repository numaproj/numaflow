package rpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	globalreducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/globalreduce/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/globalreduce/v1/globalreducemock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient/globalreducer"
	"github.com/numaproj/numaflow/pkg/window"
)

func NewMockUDSGRPCBasedGlobalReduce(mockClient *globalreducemock.MockGlobalReduceClient) *GRPCBasedGlobalReduce {
	c, _ := globalreducer.NewFromClient(mockClient)
	return &GRPCBasedGlobalReduce{c}
}

func TestGRPCBasedGlobalReduce_WaitUntilReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := globalreducemock.NewMockGlobalReduceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&globalreducepb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedGlobalReduce(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedUDF_BasicGlobalReduceWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := globalreducemock.NewMockGlobalReduceClient(ctrl)
		mockReduceClient := globalreducemock.NewMockGlobalReduce_GlobalReduceFnClient(ctrl)

		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&globalreducepb.GlobalReduceResponse{
			Result: &globalreducepb.GlobalReduceResponse_Result{
				Keys:      []string{"reduced_result_key_1"},
				Value:     []byte(`forward_message`),
				EventTime: timestamppb.New(time.Unix(120, 0).Add(-1 * time.Millisecond)),
			},
			KeyedWindow: &globalreducepb.KeyedWindow{
				Start: timestamppb.New(time.Unix(60, 0)),
				End:   timestamppb.New(time.Unix(120, 0)),
				Slot:  "test",
			},
		}, nil).Times(1)
		mockReduceClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

		requestsCh := make(chan *window.TimedWindowRequest)
		mockClient.EXPECT().GlobalReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedGlobalReduce(mockClient)
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

		mockClient := globalreducemock.NewMockGlobalReduceClient(ctrl)
		mockReduceClient := globalreducemock.NewMockGlobalReduce_GlobalReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&globalreducepb.GlobalReduceResponse{
			Result: &globalreducepb.GlobalReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, errors.New("mock error for reduce")).Times(1)
		mockReduceClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

		mockClient.EXPECT().GlobalReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedGlobalReduce(mockClient)

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

		mockClient := globalreducemock.NewMockGlobalReduceClient(ctrl)
		mockReduceClient := globalreducemock.NewMockGlobalReduce_GlobalReduceFnClient(ctrl)
		mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockReduceClient.EXPECT().Recv().Return(&globalreducepb.GlobalReduceResponse{
			Result: &globalreducepb.GlobalReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, nil).Times(1)

		mockReduceClient.EXPECT().Recv().Return(&globalreducepb.GlobalReduceResponse{
			Result: &globalreducepb.GlobalReduceResponse_Result{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		}, io.EOF).Times(1)

		mockClient.EXPECT().GlobalReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		u := NewMockUDSGRPCBasedGlobalReduce(mockClient)

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

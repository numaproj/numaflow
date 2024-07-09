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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	batchmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/batchmap/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/batchmap/v1/batchmapmock"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/batchmapper"
)

func NewMockUDSGRPCBasedBatchMap(mockClient *batchmapmock.MockBatchMapClient) *GRPCBasedBatchMap {
	c, _ := batchmapper.NewFromClient(mockClient)
	return NewUDSgRPCBasedBatchMap("test-vertex", c)
}

func TestGRPCBasedBatchMap_WaitUntilReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := batchmapmock.NewMockBatchMapClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&batchmappb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedBatchMap(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedBatchMap_BasicBatchMapFnWithMockClient(t *testing.T) {
	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := batchmapmock.NewMockBatchMapClient(ctrl)
		mockBatchMapClient := batchmapmock.NewMockBatchMap_BatchMapFnClient(ctrl)
		mockBatchMapClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockBatchMapClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockBatchMapClient.EXPECT().Recv().Return(nil, errors.New("mock error for map")).Times(1)

		mockClient.EXPECT().BatchMapFn(gomock.Any(), gomock.Any()).Return(mockBatchMapClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go func() {
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedBatchMap(mockClient)
		//readMessages := testutils.BuildTestReadMessages(2, time.Unix(1661169600, 0), nil)

		dataMessages := make([]*isb.ReadMessage, 0)

		_, err := u.ApplyBatchMap(ctx, dataMessages)
		assert.ErrorIs(t, err, &ApplyUDFErr{
			UserUDFErr: false,
			Message:    err.Error(),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})

	t.Run("test context close", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := batchmapmock.NewMockBatchMapClient(ctrl)
		mockBatchMapClient := batchmapmock.NewMockBatchMap_BatchMapFnClient(ctrl)
		mockBatchMapClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
		mockBatchMapClient.EXPECT().CloseSend().Return(nil).AnyTimes()
		mockClient.EXPECT().BatchMapFn(gomock.Any(), gomock.Any()).Return(mockBatchMapClient, nil)
		mockBatchMapClient.EXPECT().Recv().Return(nil, errors.New("mock error for map")).Times(1)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		u := NewMockUDSGRPCBasedBatchMap(mockClient)

		dataMessages := make([]*isb.ReadMessage, 0)
		// explicit cancel the context, we should see that error
		cancel()
		_, err := u.ApplyBatchMap(ctx, dataMessages)
		assert.Error(t, err, ctx.Err())
	})
}

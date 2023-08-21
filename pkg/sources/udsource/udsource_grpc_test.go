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

package udsource

import (
	"context"
	"fmt"
	"testing"
	"time"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/source/clienttest"

	"github.com/golang/mock/gomock"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1/sourcemock"
	"github.com/stretchr/testify/assert"
)

func NewMockUDSgRPCBasedUDSource(mockClient *sourcemock.MockSourceClient) *UDSgRPCBasedUDSource {
	c, _ := clienttest.New(mockClient)
	return &UDSgRPCBasedUDSource{c}
}

func Test_gRPCBasedUDSource_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sourcemock.NewMockSourceClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sourcepb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSgRPCBasedUDSource(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func Test_gRPCBasedUDSource_ApplyPendingWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testResponse := &sourcepb.PendingResponse{
			Result: &sourcepb.PendingResponse_Result{
				Count: 123,
			},
		}

		mockSourceClient := sourcemock.NewMockSourceClient(ctrl)
		mockSourceClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(testResponse, nil).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockSourceClient)
		count, err := u.ApplyPendingFn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), count)
	})

	t.Run("test err", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testResponse := &sourcepb.PendingResponse{
			Result: &sourcepb.PendingResponse_Result{
				Count: 123,
			},
		}

		mockSourceErrClient := sourcemock.NewMockSourceClient(ctrl)
		mockSourceErrClient.EXPECT().PendingFn(gomock.Any(), gomock.Any()).Return(testResponse, fmt.Errorf("mock udsource pending error")).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSource(mockSourceErrClient)
		count, err := u.ApplyPendingFn(ctx)

		assert.Equal(t, isb.PendingNotAvailable, count)
		assert.Equal(t, fmt.Errorf("mock udsource pending error"), err)
	})
}

func Test_gRPCBasedUDSource_ApplyReadWithMockClient(t *testing.T) {
	// TODO(udsource): implement this test
}

func Test_gRPCBasedUDSource_ApplyAckWithMockClient(t *testing.T) {
	// TODO(udsource): implement this test
}

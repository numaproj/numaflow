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
	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1/mapmock"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/sdkclient/batchmapper"
)

func NewMockUDSGRPCBasedBatchMap(mockClient *mapmock.MockMapClient) *GRPCBasedBatchMap {
	c, _ := batchmapper.NewFromClient(mockClient)
	return &GRPCBasedBatchMap{"test-vertex", c}
}

func TestGRPCBasedBatchMap_WaitUntilReady(t *testing.T) {
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

	u := NewMockUDSGRPCBasedBatchMap(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

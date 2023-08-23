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

package udsideinput

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sideinputpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1/sideinputmock"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/sdkclient/sideinput"
)

func NewMockUDSgRPCBasedUDSideinput(mockClient *sideinputmock.MockSideInputClient) *UDSgRPCBasedUDSideinput {
	c, _ := sideinput.NewFromClient(mockClient)
	return &UDSgRPCBasedUDSideinput{c}
}

func Test_gRPCBasedUDSideInput_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := sideinputmock.NewMockSideInputClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&sideinputpb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSgRPCBasedUDSideinput(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func Test_gRPCBasedUDSideInput_ApplyWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resp := sideinputpb.SideInputResponse{Value: []byte(`sideinput_message`)}

		mockSideInputClient := sideinputmock.NewMockSideInputClient(ctrl)
		mockSideInputClient.EXPECT().RetrieveSideInput(gomock.Any(), gomock.Any()).Return(&sideinputpb.SideInputResponse{Value: []byte("sideinput_message")}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSgRPCBasedUDSideinput(mockSideInputClient)
		ret, err := u.Apply(ctx)
		assert.NoError(t, err)
		assert.Equal(t, resp.Value, ret.Value)
	})
}

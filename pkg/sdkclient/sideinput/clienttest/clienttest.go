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

package clienttest

import (
	"context"
	"fmt"

	sideinputsdk "github.com/numaproj/numaflow/pkg/sdkclient/sideinput/client"

	sideinputpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1/sideinputmock"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc client for testing.
type client struct {
	grpcClt sideinputpb.UserDefinedSideInputClient
}

var _ sideinputsdk.Client = (*client)(nil)

// New creates a new mock client object.
func New(c *sideinputmock.MockUserDefinedSideInputClient) (*client, error) {
	return &client{c}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
	return nil
}

// IsReady returns true if the grpc connection is ready to use.
func (c *client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

// RetrieveSideInput retrieves side input from the grpc server for the user defined side input function.
func (c *client) RetrieveSideInput(ctx context.Context, in *emptypb.Empty) (*sideinputpb.SideInputResponse, error) {
	resp, err := c.grpcClt.RetrieveSideInput(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to execute c.grpcClt.RetrieveSideInput(): %w", err)
	}
	return resp, nil
}

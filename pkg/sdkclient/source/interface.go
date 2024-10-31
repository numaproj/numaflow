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

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client.
type Client interface {
	// CloseConn closes the gRPC client connection.
	CloseConn(ctx context.Context) error
	// IsReady checks if the udsource connection is ready.
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	// ReadFn reads messages from the udsource.
	ReadFn(ctx context.Context, req *sourcepb.ReadRequest, datumCh chan<- *sourcepb.ReadResponse) error
	// AckFn acknowledges messages from the udsource.
	AckFn(ctx context.Context, req *sourcepb.AckRequest) (*sourcepb.AckResponse, error)
	// PendingFn returns the number of pending messages from the udsource.
	PendingFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PendingResponse, error)
	// PartitionsFn returns the list of partitions from the udsource.
	PartitionsFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PartitionsResponse, error)
}

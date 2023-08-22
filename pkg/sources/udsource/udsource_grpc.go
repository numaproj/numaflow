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
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	sourceclient "github.com/numaproj/numaflow/pkg/sdkclient/source/client"

	"google.golang.org/protobuf/types/known/emptypb"
)

// GRPCBasedUDSource applies a user-defined source over gRPC
// connection where server is the UDSource.
type GRPCBasedUDSource struct {
	client sourceclient.Client
}

// NewUDSgRPCBasedUDSource accepts a gRPC client and returns a new GRPCBasedUDSource.
func NewUDSgRPCBasedUDSource(c sourceclient.Client) (*GRPCBasedUDSource, error) {
	return &GRPCBasedUDSource{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedUDSource) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsource is healthy.
func (u *GRPCBasedUDSource) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsource is connected.
func (u *GRPCBasedUDSource) WaitUntilReady(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// ApplyPendingFn returns the number of pending messages in the source.
func (u *GRPCBasedUDSource) ApplyPendingFn(ctx context.Context) (int64, error) {
	if resp, err := u.client.PendingFn(ctx, &emptypb.Empty{}); err == nil {
		return int64(resp.Result.Count), nil
	} else {
		return isb.PendingNotAvailable, err
	}
}

// ApplyReadFn reads messages from the source.
func (u *GRPCBasedUDSource) ApplyReadFn(_ context.Context, _ int64) ([]*isb.ReadMessage, error) {
	// TODO(udsource) - Implement it
	return nil, nil
}

// ApplyAckFn acknowledges messages in the source.
func (u *GRPCBasedUDSource) ApplyAckFn(_ context.Context, _ []isb.Offset) []error {
	// TODO(udsource) - Implement it
	return nil
}

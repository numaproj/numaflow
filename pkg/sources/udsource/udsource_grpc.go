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

// UDSgRPCBasedUDSource applies a user-defined source over gRPC
// (over Unix Domain Socket) client/server where server is the UDSource.
type UDSgRPCBasedUDSource struct {
	client sourceclient.Client
}

// NewUDSgRPCBasedUDSource returns UDSgRPCBasedUDSource
func NewUDSgRPCBasedUDSource() (*UDSgRPCBasedUDSource, error) {
	c, err := sourceclient.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &UDSgRPCBasedUDSource{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedUDSource) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsource is healthy.
func (u *UDSgRPCBasedUDSource) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsource is connected.
func (u *UDSgRPCBasedUDSource) WaitUntilReady(ctx context.Context) error {
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
func (u *UDSgRPCBasedUDSource) ApplyPendingFn(ctx context.Context) (int64, error) {
	if resp, err := u.client.PendingFn(ctx, &emptypb.Empty{}); err == nil {
		return int64(resp.Result.Count), nil
	} else {
		return isb.PendingNotAvailable, err
	}
}

// ApplyReadFn reads messages from the source.
func (u *UDSgRPCBasedUDSource) ApplyReadFn(_ context.Context, _ int64) ([]*isb.ReadMessage, error) {
	// TODO(udsource) - Implement it
	return nil, nil
}

// ApplyAckFn acknowledges messages in the source.
func (u *UDSgRPCBasedUDSource) ApplyAckFn(_ context.Context, _ []isb.Offset) []error {
	// TODO(udsource) - Implement it
	return nil
}

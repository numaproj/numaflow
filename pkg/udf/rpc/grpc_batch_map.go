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
	"fmt"
	"time"

	batchmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/batchmapper"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedBatchMap is a map applier that uses gRPC client to invoke the map UDF. It implements the applier.MapApplier interface.
type GRPCBasedBatchMap struct {
	vertexName string
	client     batchmapper.Client
}

func NewUDSgRPCBasedBatchMap(vertexName string, client batchmapper.Client) *GRPCBasedBatchMap {
	return &GRPCBasedBatchMap{
		vertexName: vertexName,
		client:     client,
	}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedBatchMap) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedBatchMap) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *GRPCBasedBatchMap) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for map udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (u *GRPCBasedBatchMap) ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]*isb.WriteMessage, error) {
	inputChan := make(chan *batchmappb.MapRequest)
	respCh, errCh := u.client.BatchMapFn(ctx, inputChan)
	// error routine
	go func() {

	}()

	// Read routine
	go func() {

	}()

	// Wait for all responses to be received
	for resp := range respCh {
		print(resp)

	}
	return nil, nil
}

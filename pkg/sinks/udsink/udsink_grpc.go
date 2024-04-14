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

package udsink

import (
	"context"
	"fmt"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	sinkclient "github.com/numaproj/numaflow/pkg/sdkclient/sinker"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

var WriteToFallbackErr = "write to fallback sink"

// SinkApplier applies the sink on the read message and gives back a response. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type SinkApplier interface {
	ApplySink(ctx context.Context, requests []*sinkpb.SinkRequest) []error
}

// UDSgRPCBasedUDSink applies user defined sink over gRPC (over Unix Domain Socket) client/server where server is the UDSink.
type UDSgRPCBasedUDSink struct {
	client sinkclient.Client
}

// NewUDSgRPCBasedUDSink returns UDSgRPCBasedUDSink
func NewUDSgRPCBasedUDSink(client sinkclient.Client) *UDSgRPCBasedUDSink {
	return &UDSgRPCBasedUDSink{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedUDSink) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsink is healthy.
func (u *UDSgRPCBasedUDSink) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsink is connected.
func (u *UDSgRPCBasedUDSink) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for udsink to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (u *UDSgRPCBasedUDSink) ApplySink(ctx context.Context, requests []*sinkpb.SinkRequest) []error {
	errs := make([]error, len(requests))

	response, err := u.client.SinkFn(ctx, requests)
	if err != nil {
		for i := range requests {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       fmt.Sprintf("gRPC client.SinkFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
		return errs
	}
	// Use ID to map the response messages, so that there's no strict requirement for the user defined sink to return the response in order.
	resMap := make(map[string]*sinkpb.SinkResponse_Result)
	for _, res := range response.GetResults() {
		resMap[res.GetId()] = res
	}
	for i, m := range requests {
		if r, existing := resMap[m.GetId()]; !existing {
			errs[i] = fmt.Errorf("not found in response")
		} else {
			if r.GetStatus() == sinkpb.Status_FAILURE {
				if r.GetErrMsg() != "" {
					errs[i] = fmt.Errorf(r.GetErrMsg())
				} else {
					errs[i] = fmt.Errorf("unsuccessful due to unknown reason")
				}
			} else if r.GetStatus() == sinkpb.Status_FALLBACK {
				errs[i] = fmt.Errorf(WriteToFallbackErr)
			} else {
				errs[i] = nil
			}
		}
	}
	return errs
}

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
	sinksdk "github.com/numaproj/numaflow-go/pkg/sink"
	"github.com/numaproj/numaflow-go/pkg/sink/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

type udsGRPCBasedUDSink struct {
	client sinksdk.Client
}

func NewUDSGRPCBasedUDSink() (*udsGRPCBasedUDSink, error) {
	c, err := client.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &udsGRPCBasedUDSink{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *udsGRPCBasedUDSink) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *udsGRPCBasedUDSink) WaitUntilReady(ctx context.Context) error {
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

func (u *udsGRPCBasedUDSink) Apply(ctx context.Context, dList []*sinkpb.Datum) []error {
	errs := make([]error, len(dList))

	responseList, err := u.client.SinkFn(ctx, dList)
	if err != nil {
		for i := range dList {
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
	// Use ID to map the response messages, so that there's no strict requirement for the user defined sink to return the responseList in order.
	resMap := make(map[string]*sinkpb.Response)
	for _, res := range responseList {
		resMap[res.GetId()] = res
	}
	for i, m := range dList {
		if r, existing := resMap[m.GetId()]; !existing {
			errs[i] = fmt.Errorf("not found in responseList")
		} else {
			if !r.Success {
				if r.GetErrMsg() != "" {
					errs[i] = fmt.Errorf(r.GetErrMsg())
				} else {
					errs[i] = fmt.Errorf("unsuccessful due to unknown reason")
				}
			}
		}
	}
	return errs
}

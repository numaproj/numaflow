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

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/isb"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedMap is a map applier that uses gRPC client to invoke the map UDF. It implements the applier.MapApplier interface.
type GRPCBasedMap struct {
	client mapper.Client
}

func NewUDSgRPCBasedMap(client mapper.Client) *GRPCBasedMap {
	return &GRPCBasedMap{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedMap) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedMap) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *GRPCBasedMap) WaitUntilReady(ctx context.Context) error {
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

func (u *GRPCBasedMap) ApplyMap(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	parentMessageInfo := readMessage.MessageInfo
	var req = &mappb.MapRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(readMessage.Watermark),
	}

	response, err := u.client.MapFn(ctx, req)
	if err != nil {
		udfErr, _ := sdkerr.FromError(err)
		switch udfErr.ErrorKind() {
		case sdkerr.Retryable:
			var success bool
			_ = wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
				// retry every "duration * factor + [0, jitter]" interval for 5 times
				Duration: 1 * time.Second,
				Factor:   1,
				Jitter:   0.1,
				Steps:    5,
			}, func(_ context.Context) (done bool, err error) {
				response, err = u.client.MapFn(ctx, req)
				if err != nil {
					udfErr, _ = sdkerr.FromError(err)
					switch udfErr.ErrorKind() {
					case sdkerr.Retryable:
						return false, nil
					case sdkerr.NonRetryable:
						return true, nil
					default:
						return true, nil
					}
				}
				success = true
				return true, nil
			})
			if !success {
				return nil, ApplyUDFErr{
					UserUDFErr: false,
					Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
					InternalErr: InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
		case sdkerr.NonRetryable:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

	writeMessages := make([]*isb.WriteMessage, 0)
	for _, result := range response.GetResults() {
		keys := result.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		writeMessages = append(writeMessages, taggedMessage)
	}
	return writeMessages, nil
}

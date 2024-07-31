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

package transformer

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/isb"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/sourcetransformer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/rpc"
)

// GRPCBasedTransformer applies user-defined transformer over gRPC (over Unix Domain Socket) client/server where server is the transformer.
type GRPCBasedTransformer struct {
	vertexName string
	client     sourcetransformer.Client
}

// NewGRPCBasedTransformer returns a new gRPCBasedTransformer object.
func NewGRPCBasedTransformer(vertexName string, client sourcetransformer.Client) *GRPCBasedTransformer {
	return &GRPCBasedTransformer{
		vertexName: vertexName,
		client:     client,
	}
}

// IsHealthy checks if the transformer container is healthy.
func (u *GRPCBasedTransformer) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *GRPCBasedTransformer) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for transformer to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedTransformer) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

func (u *GRPCBasedTransformer) ApplyTransform(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentMessageInfo := readMessage.MessageInfo
	var req = &v1.SourceTransformRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(readMessage.Watermark),
		Headers:   readMessage.Headers,
	}

	response, err := u.client.SourceTransformFn(ctx, req)
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
				response, err = u.client.SourceTransformFn(ctx, req)
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
				return nil, &rpc.ApplyUDFErr{
					UserUDFErr: false,
					Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
					InternalErr: rpc.InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
		case sdkerr.NonRetryable:
			return nil, &rpc.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
				InternalErr: rpc.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, &rpc.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
				InternalErr: rpc.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

	taggedMessages := make([]*isb.WriteMessage, 0)
	for i, result := range response.GetResults() {
		keys := result.Keys
		if result.EventTime != nil {
			// Transformer supports changing event time.
			parentMessageInfo.EventTime = result.EventTime.AsTime()
		}
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID: isb.MessageID{
						VertexName: u.vertexName,
						Offset:     offset.String(),
						Index:      int32(i),
					},
					Keys: keys,
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}
	return taggedMessages, nil
}

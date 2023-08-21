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

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/udf/client"
	"github.com/numaproj/numaflow/pkg/udf/function"
)

// GRPCBasedTransformer applies user defined transformer over gRPC (over Unix Domain Socket) client/server where server is the transformer.
type GRPCBasedTransformer struct {
	client client.Client
}

var _ applier.MapApplier = (*GRPCBasedTransformer)(nil)

// NewGRPCBasedTransformer returns a new gRPCBasedTransformer object.
func NewGRPCBasedTransformer() (*GRPCBasedTransformer, error) {
	c, err := client.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &GRPCBasedTransformer{c}, nil
}

// NewGRPCBasedTransformerWithClient need this for testing
func NewGRPCBasedTransformerWithClient(client client.Client) *GRPCBasedTransformer {
	return &GRPCBasedTransformer{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedTransformer) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the transformer container is healthy.
func (u *GRPCBasedTransformer) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *GRPCBasedTransformer) WaitUntilReady(ctx context.Context) error {
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

func (u *GRPCBasedTransformer) ApplyMap(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentMessageInfo := readMessage.MessageInfo
	var d = &functionpb.DatumRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentMessageInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(readMessage.Watermark)},
	}

	datumList, err := u.client.MapTFn(ctx, d)
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
			}, func() (done bool, err error) {
				datumList, err = u.client.MapTFn(ctx, d)
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
				return nil, function.ApplyUDFErr{
					UserUDFErr: false,
					Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
					InternalErr: function.InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
		case sdkerr.NonRetryable:
			return nil, function.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: function.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, function.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: function.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

	taggedMessages := make([]*isb.WriteMessage, 0)
	for i, datum := range datumList {
		keys := datum.Keys
		if datum.EventTime != nil {
			// Transformer supports changing event time.
			parentMessageInfo.EventTime = datum.EventTime.EventTime.AsTime()
		}
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: datum.Value,
				},
			},
			Tags: datum.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}
	return taggedMessages, nil
}

func (u *GRPCBasedTransformer) ApplyMapStream(_ context.Context, _ *isb.ReadMessage, _ chan<- isb.WriteMessage) error {
	return fmt.Errorf("method ApplyMapStream not implemented")
}

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

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/client"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/udf/function"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// gRPCBasedTransformer applies user defined transformer over gRPC (over Unix Domain Socket) client/server where server is the transformer.
type gRPCBasedTransformer struct {
	client functionsdk.Client
}

var _ applier.MapApplier = (*gRPCBasedTransformer)(nil)

// NewGRPCBasedTransformer returns a new gRPCBasedTransformer object.
func NewGRPCBasedTransformer() (*gRPCBasedTransformer, error) {
	c, err := client.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &gRPCBasedTransformer{c}, nil
}

// NewGRPCBasedTransformerWithClient need this for testing
func NewGRPCBasedTransformerWithClient(client functionsdk.Client) *gRPCBasedTransformer {
	return &gRPCBasedTransformer{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *gRPCBasedTransformer) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the transformer container is healthy.
func (u *gRPCBasedTransformer) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *gRPCBasedTransformer) WaitUntilReady(ctx context.Context) error {
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

func (u *gRPCBasedTransformer) ApplyMap(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
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
		return nil, function.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapTFn failed, %s", err),
			InternalErr: function.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
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

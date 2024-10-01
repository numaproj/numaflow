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
	"errors"
	"fmt"
	"log"
	"time"

	v1 "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
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

var errSourceTransformFnEmptyMsgId = errors.New("response from SourceTransformFn doesn't contain a message id")

func (u *GRPCBasedTransformer) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	var transformResults []isb.ReadWriteMessagePair
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	requests := make([]*v1.SourceTransformRequest, 0, len(messages))
	idToMsgMapping := make(map[string]*isb.ReadMessage)
	for _, msg := range messages {
		id := msg.Message.ID.String()
		idToMsgMapping[id] = msg
		req := &v1.SourceTransformRequest{
			Request: &v1.SourceTransformRequest_Request{
				Keys:      msg.Keys,
				Value:     msg.Body.Payload,
				EventTime: timestamppb.New(msg.MessageInfo.EventTime),
				Watermark: timestamppb.New(msg.Watermark),
				Headers:   msg.Headers,
				Id:        id,
			},
		}
		requests = append(requests, req)
	}

	log.Println("Sending message to source transform client")
	responses, err := u.client.SourceTransformFn(ctx, requests)
	log.Println("Received responses from source transform client")

	if err != nil {
		err = &rpc.ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
			InternalErr: rpc.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
		return nil, err
	}

	var taggedMessages []*isb.WriteMessage
	for _, resp := range responses {
		parentMessage, ok := idToMsgMapping[resp.GetId()]
		if !ok {
			panic("tracker doesn't contain the message ID received from the response")
		}
		for i, result := range resp.GetResults() {
			keys := result.Keys
			if result.EventTime != nil {
				// Transformer supports changing event time.
				log.Println("Updating event time from ", parentMessage.MessageInfo.EventTime.UnixMilli(), " to ", result.EventTime.AsTime().UnixMilli())
				parentMessage.MessageInfo.EventTime = result.EventTime.AsTime()
			}
			taggedMessage := &isb.WriteMessage{
				Message: isb.Message{
					Header: isb.Header{
						MessageInfo: parentMessage.MessageInfo,
						ID: isb.MessageID{
							VertexName: u.vertexName,
							Offset:     parentMessage.ReadOffset.String(),
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
		responsePair := isb.ReadWriteMessagePair{
			ReadMessage:   parentMessage,
			WriteMessages: taggedMessages,
			Err:           nil,
		}
		transformResults = append(transformResults, responsePair)
	}
	return transformResults, nil
}

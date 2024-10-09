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

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedMap is a map applier that uses gRPC client to invoke the map UDF. It implements the applier.MapApplier interface.
type GRPCBasedMap struct {
	vertexName string
	client     mapper.Client
}

func NewUDSgRPCBasedMap(vertexName string, client mapper.Client) *GRPCBasedMap {
	return &GRPCBasedMap{
		vertexName: vertexName,
		client:     client,
	}
}

// Close closes the gRPC client connection.
func (u *GRPCBasedMap) Close() error {
	return u.client.CloseConn()
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

func (u *GRPCBasedMap) ApplyMap(ctx context.Context, readMessages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	requests := make([]*mappb.MapRequest, len(readMessages))
	results := make([]isb.ReadWriteMessagePair, len(readMessages))
	idToMsgMapping := make(map[string]*isb.ReadMessage)

	for i, msg := range readMessages {
		// we track the id to the message mapping to be able to match the response with the original message.
		// message info of response should be the same as the message info of the request.
		id := msg.ReadOffset.String()
		idToMsgMapping[id] = msg
		req := &mappb.MapRequest{
			Request: &mappb.MapRequest_Request{
				Keys:      msg.Keys,
				Value:     msg.Body.Payload,
				EventTime: timestamppb.New(msg.MessageInfo.EventTime),
				Watermark: timestamppb.New(msg.Watermark),
				Headers:   msg.Headers,
			},
			Id: id,
		}
		requests[i] = req
	}

	responses, err := u.client.MapFn(ctx, requests)

	if err != nil {
		println("gRPC client.mapFn failed, ", err.Error())
		err = &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
		return nil, err
	}

	for i, resp := range responses {
		parentMessage, ok := idToMsgMapping[resp.GetId()]
		if !ok {
			panic(fmt.Sprintf("tracker doesn't contain the message ID received from the response: %s", resp.GetId()))
		}
		taggedMessages := make([]*isb.WriteMessage, len(resp.GetResults()))
		for j, result := range resp.GetResults() {
			keys := result.Keys
			taggedMessage := &isb.WriteMessage{
				Message: isb.Message{
					Header: isb.Header{
						MessageInfo: parentMessage.MessageInfo,
						ID: isb.MessageID{
							VertexName: u.vertexName,
							Offset:     parentMessage.ReadOffset.String(),
							Index:      int32(j),
						},
						Keys: keys,
					},
					Body: isb.Body{
						Payload: result.Value,
					},
				},
				Tags: result.Tags,
			}
			taggedMessage.Headers = parentMessage.Headers
			taggedMessages[j] = taggedMessage
		}
		results[i] = isb.ReadWriteMessagePair{
			ReadMessage:   parentMessage,
			WriteMessages: taggedMessages,
		}
	}
	return results, nil
}

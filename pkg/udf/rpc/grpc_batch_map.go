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
	"google.golang.org/protobuf/types/known/timestamppb"

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

func (u *GRPCBasedBatchMap) ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	logger := logging.FromContext(ctx)
	udfResults := make([]isb.ReadWriteMessagePair, len(messages))
	inputChan := make(chan *batchmappb.MapRequest)
	requestTracker := NewTracker()
	respCh, errCh := u.client.BatchMapFn(ctx, inputChan)

	// Read routine
	go func() {
		defer close(inputChan)
		for _, msg := range messages {
			requestTracker.AddRequest(msg)
			inputChan <- u.parseInputRequest(msg)
		}
	}()

	//var wg sync.WaitGroup
	// Wait for all responses to be received
	idx := 0
loop:
	for {
		select {
		case err := <-errCh:
			// convertToSDK()
			return nil, err
		case grpcResp, ok := <-respCh:
			if !ok {
				break loop
			}
			msgId := grpcResp.GetId()
			parentMessage, ok := requestTracker.GetRequest(msgId)
			if !ok {
				logger.Info("MYDEBUG: Request missing from tracker")
			}
			//wg.Add(1)
			parsedResp := u.parseResponse(grpcResp, parentMessage)
			udfResults[idx].WriteMessages = parsedResp
			udfResults[idx].ReadMessage = parentMessage
			requestTracker.RemoveRequest(msgId)
			idx += 1
		}
	}
	//wg.Wait()
	return udfResults, nil
}

func (u *GRPCBasedBatchMap) parseResponse(response *batchmappb.BatchMapResponse, parentMessage *isb.ReadMessage) []*isb.WriteMessage {
	writeMessages := make([]*isb.WriteMessage, 0)
	for index, result := range response.GetResults() {
		keys := result.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessage.MessageInfo,
					Keys:        keys,
					ID: isb.MessageID{
						VertexName: u.vertexName,
						Offset:     parentMessage.ReadOffset.String(),
						Index:      int32(index),
					},
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		writeMessages = append(writeMessages, taggedMessage)
	}
	return writeMessages
}

func (u *GRPCBasedBatchMap) parseInputRequest(inputMsg *isb.ReadMessage) *batchmappb.MapRequest {
	keys := inputMsg.Keys
	payload := inputMsg.Body.Payload
	parentMessageInfo := inputMsg.MessageInfo
	var req = &batchmappb.MapRequest{
		Id:        inputMsg.ReadOffset.String(),
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(inputMsg.Watermark),
		Headers:   inputMsg.Headers,
	}
	return req
}

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

	batchmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/batchmap/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/tracker"
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
				log.Infof("Waiting for batch map udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// ApplyBatchMap is used to invoke the BatchMapFn RPC using the client.
// It applies the batch map udf on an array read messages and sends the responses for the whole batch.
func (u *GRPCBasedBatchMap) ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	logger := logging.FromContext(ctx)
	// udfResults is structure used to store the results corresponding to the read messages.
	udfResults := make([]isb.ReadWriteMessagePair, 0)

	// inputChan is used to stream messages to the grpc client
	inputChan := make(chan *batchmappb.BatchMapRequest)

	// Invoke the RPC from the client
	respCh, errCh := u.client.BatchMapFn(ctx, inputChan)

	// trackerReq is used to store the read messages in a key, value manner where
	// key is the read offset and the reference to read message as the value.
	// Once the results are received from the UDF, we map the responses to the corresponding request
	// using a lookup on this Tracker.
	trackerReq := tracker.NewMessageTracker(messages)

	// Read routine: this goroutine iterates over the input messages and sends each
	// of the read messages to the grpc client after transforming it to a BatchMapRequest.
	// Once all messages are sent, it closes the input channel to indicate that all requests have been read.
	// On creating a new request, we add it to a Tracker map so that the responses on the stream
	// can be mapped backed to the given parent request
	go func() {
		defer close(inputChan)
		for _, msg := range messages {
			inputChan <- u.parseInputRequest(msg)
		}
	}()

	// Process the responses received on the response channel:
	// This is an infinite loop which would exit
	// 1. Once there are no more responses left to read from the channel
	// 2. There is an error received on the error channel
	// We have not added a case for context.Done as there is a handler for that in the client, and it should
	// propagate it on the error channel itself.
	//
	// On getting a response, it would parse them and create a new ReadWriteMessagePair entry in the udfResults
	// Any errors received from the client, are propagated back to the caller.
loop:
	for {
		select {
		// got an error on the error channel, so return immediately
		case err := <-errCh:
			err = &ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.BatchMapFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
			return nil, err
		case grpcResp, ok := <-respCh:
			// if there are no more messages to read on the channel we can break
			if !ok {
				break loop
			}
			// Get the unique request ID for which these responses are meant for.
			msgId := grpcResp.GetId()
			// Fetch the request value for the given ID from the Tracker
			parentMessage := trackerReq.Remove(msgId)
			if parentMessage == nil {
				// this case is when the given request ID was not present in the Tracker.
				// This means that either the UDF added an incorrect ID
				// This cannot be processed further and should result in an error
				// Can there be another case for this?
				logger.Error("Request missing from message tracker, ", msgId)
				return nil, fmt.Errorf("incorrect ID found during batch map processing")
			}
			// parse the responses received
			// TODO(map-batch): should we make this concurrent by using multiple goroutines, instead of sequential.
			// Try and see if any perf improvements from this.
			parsedResp := u.parseResponse(grpcResp, parentMessage)
			responsePair := isb.ReadWriteMessagePair{
				ReadMessage:   parentMessage,
				WriteMessages: parsedResp,
				Err:           nil,
			}
			udfResults = append(udfResults, responsePair)
		}
	}
	// check if there are elements left in the Tracker. This cannot be an acceptable case as we want the
	// UDF to send responses for all elements.
	if !trackerReq.IsEmpty() {
		logger.Error("BatchMap response for all requests not received from UDF")
		return nil, fmt.Errorf("batchMap response for all requests not received from UDF")
	}
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
						Offset:     response.GetId(),
						Index:      int32(index),
					},
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		// set the headers for the write messages
		taggedMessage.Headers = parentMessage.Headers
		writeMessages = append(writeMessages, taggedMessage)
	}
	return writeMessages
}

func (u *GRPCBasedBatchMap) parseInputRequest(inputMsg *isb.ReadMessage) *batchmappb.BatchMapRequest {
	keys := inputMsg.Keys
	payload := inputMsg.Body.Payload
	parentMessageInfo := inputMsg.MessageInfo
	var req = &batchmappb.BatchMapRequest{
		Id:        inputMsg.ReadOffset.String(),
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(inputMsg.Watermark),
		Headers:   inputMsg.Headers,
	}
	return req
}

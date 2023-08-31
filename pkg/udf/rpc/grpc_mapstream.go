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

	mapstreampb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapstreamer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedMapStream is a map stream applier that uses gRPC client to invoke the map stream UDF. It implements the applier.MapStreamApplier interface.
type GRPCBasedMapStream struct {
	client mapstreamer.Client
}

func NewUDSgRPCBasedMapStream(client mapstreamer.Client) *GRPCBasedMapStream {
	return &GRPCBasedMapStream{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedMapStream) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map stream udf is healthy.
func (u *GRPCBasedMapStream) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map stream udf is connected.
func (u *GRPCBasedMapStream) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for map stream udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (u *GRPCBasedMapStream) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	defer close(writeMessageCh)

	keys := message.Keys
	payload := message.Body.Payload
	offset := message.ReadOffset
	parentMessageInfo := message.MessageInfo

	var d = &mapstreampb.MapStreamRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(message.Watermark),
	}

	responseCh := make(chan *mapstreampb.MapStreamResponse)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		err := u.client.MapStreamFn(ctx, d, responseCh)
		if err != nil {
			err = ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapStreamFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
			return err
		}
		return nil
	})

	i := 0
	for response := range responseCh {
		result := response.Result
		i++
		keys := result.GetKeys()
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.GetValue(),
				},
			},
			Tags: result.GetTags(),
		}
		writeMessageCh <- *taggedMessage
	}

	return errs.Wait()
}

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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mapstreampb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1/mapstreammock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapstreamer"
)

func NewMockUDSGRPCBasedMapStream(mockClient *mapstreammock.MockMapStreamClient) *GRPCBasedMapStream {
	c, _ := mapstreamer.NewFromClient(mockClient)
	return &GRPCBasedMapStream{c}
}

func TestGRPCBasedMapStream_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mapstreampb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedMapStream(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedUDF_BasicApplyStreamWithMockClient(t *testing.T) {
	t.Run("test success", func(t *testing.T) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
		mockMapStreamClient := mapstreammock.NewMockMapStream_MapStreamFnClient(ctrl)

		req := &mapstreampb.MapStreamRequest{
			Keys:      []string{"test_success_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169600, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}
		expectedDatum := &mapstreampb.MapStreamResponse{
			Result: &mapstreampb.MapStreamResponse_Result{
				Keys:  []string{"test_success_key"},
				Value: []byte(`forward_message`),
			},
		}
		mockMapStreamClient.EXPECT().Recv().Return(expectedDatum, nil).Times(1)
		mockMapStreamClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

		mockClient.EXPECT().MapStreamFn(gomock.Any(), &rpcMsg{msg: req}).Return(mockMapStreamClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		writeMessageCh := make(chan isb.WriteMessage)
		u := NewMockUDSGRPCBasedMapStream(mockClient)

		go func() {
			err := u.ApplyMapStream(ctx, &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						MessageInfo: isb.MessageInfo{
							EventTime: time.Unix(1661169600, 0),
						},
						ID:   "test_id",
						Keys: []string{"test_success_key"},
					},
					Body: isb.Body{
						Payload: []byte(`forward_message`),
					},
				},
				ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
			}, writeMessageCh)
			assert.NoError(t, err)
		}()

		for msg := range writeMessageCh {
			assert.Equal(t, req.Keys, msg.Keys)
			assert.Equal(t, req.Value, msg.Payload)
		}
	})

	t.Run("test error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
		mockMapStreamClient := mapstreammock.NewMockMapStream_MapStreamFnClient(ctrl)

		req := &mapstreampb.MapStreamRequest{
			Keys:      []string{"test_error_key"},
			Value:     []byte(`forward_message`),
			EventTime: timestamppb.New(time.Unix(1661169660, 0)),
			Watermark: timestamppb.New(time.Time{}),
		}

		mockMapStreamClient.EXPECT().Recv().Return(
			&mapstreampb.MapStreamResponse{
				Result: &mapstreampb.MapStreamResponse_Result{
					Keys:  []string{"test_error_key"},
					Value: []byte(`forward_message`),
				},
			}, errors.New("mock error for map")).AnyTimes()

		mockClient.EXPECT().MapStreamFn(gomock.Any(), &rpcMsg{msg: req}).Return(mockMapStreamClient, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded {
				t.Log(t.Name(), "test timeout")
			}
		}()

		u := NewMockUDSGRPCBasedMapStream(mockClient)
		writeMessageCh := make(chan isb.WriteMessage)

		err := u.ApplyMapStream(ctx, &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID:   "test_id",
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		}, writeMessageCh)
		assert.ErrorIs(t, err, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("%s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		})
	})
}

func TestHGRPCBasedUDF_ApplyStreamWithMockClient(t *testing.T) {
	multiplyBy2 := func(body []byte) interface{} {
		var result testutils.PayloadForTest
		_ = json.Unmarshal(body, &result)
		result.Value = result.Value * 2
		return result
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var count = int64(1)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1661169600, 0))

	mockClient := mapstreammock.NewMockMapStreamClient(ctrl)
	mockMapStreamClient := mapstreammock.NewMockMapStream_MapStreamFnClient(ctrl)
	for _, message := range readMessages {
		keys := message.Keys
		payload := message.Body.Payload
		parentMessageInfo := message.MessageInfo
		var datum = &mapstreampb.MapStreamRequest{
			Keys:      keys,
			Value:     payload,
			EventTime: timestamppb.New(parentMessageInfo.EventTime),
			Watermark: timestamppb.New(message.Watermark),
		}
		mockMapStreamClient.EXPECT().Recv().DoAndReturn(
			func() (*mapstreampb.MapStreamResponse, error) {
				var originalValue testutils.PayloadForTest
				_ = json.Unmarshal(datum.GetValue(), &originalValue)
				doubledValue, _ := json.Marshal(multiplyBy2(datum.GetValue()).(testutils.PayloadForTest))
				var element *mapstreampb.MapStreamResponse_Result
				if originalValue.Value%2 == 0 {
					element = &mapstreampb.MapStreamResponse_Result{
						Keys:  []string{"even"},
						Value: doubledValue,
					}
				} else {
					element = &mapstreampb.MapStreamResponse_Result{
						Keys:  []string{"odd"},
						Value: doubledValue,
					}
				}

				response := &mapstreampb.MapStreamResponse{
					Result: element,
				}

				return response, nil
			},
		).Times(1)
	}
	mockMapStreamClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

	mockClient.EXPECT().MapStreamFn(gomock.Any(), gomock.Any()).Return(mockMapStreamClient, nil).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedMapStream(mockClient)

	var results = make([][]byte, len(readMessages))
	var resultKeys = make([][]string, len(readMessages))
	idx := 0
	for _, readMessage := range readMessages {
		writeMessageCh := make(chan isb.WriteMessage)
		go func() {
			err := u.ApplyMapStream(ctx, &readMessage, writeMessageCh)
			assert.NoError(t, err)
		}()
		for m := range writeMessageCh {
			results[idx] = m.Payload
			resultKeys[idx] = m.Header.Keys
			idx++
		}
	}

	var expectedResults = make([][]byte, count)
	var expectedKeys = make([][]string, count)
	for idx, readMessage := range readMessages {
		var readMessagePayload testutils.PayloadForTest
		_ = json.Unmarshal(readMessage.Payload, &readMessagePayload)
		if readMessagePayload.Value%2 == 0 {
			expectedKeys[idx] = []string{"even"}
		} else {
			expectedKeys[idx] = []string{"odd"}
		}
		marshal, _ := json.Marshal(multiplyBy2(readMessage.Payload))
		expectedResults[idx] = marshal
	}

	assert.Equal(t, expectedResults, results)
	assert.Equal(t, expectedKeys, resultKeys)
}

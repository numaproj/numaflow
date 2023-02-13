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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

var testEventTime, testReadOffset, testWatermark = time.Date(2023, 1, 13, 1, 1, 1, 1, time.UTC),
	isb.SimpleIntOffset(func() int64 { return int64(1) }),
	time.Date(2022, 1, 13, 1, 1, 1, 1, time.UTC)

var testReadOffset2, testWatermark2 = isb.SimpleIntOffset(func() int64 { return int64(2) }),
	time.Date(2022, 2, 13, 1, 1, 1, 1, time.UTC)

var testReadOffset3, testWatermark3 = isb.SimpleIntOffset(func() int64 { return int64(3) }),
	time.Date(2022, 3, 13, 1, 1, 1, 1, time.UTC)

func TestTransformer(t *testing.T) {
	tests := []struct {
		name         string
		mapper       applier.MapApplier
		input        []*isb.ReadMessage
		expectOutput []*isb.ReadMessage
		cancelledCtx bool
	}{
		{
			name: "given_one_input_message_when_transform_successfully_then_event_time_changed",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				message := input.Message
				message.EventTime = testEventTime
				output = append(output, &message)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
			},
			expectOutput: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
			},
		},
		{
			name: "given_context_already_cancelled_when_transform_return_empty_slice",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				message := input.Message
				message.EventTime = time.Time{}
				output = append(output, &message)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
			},
			expectOutput: []*isb.ReadMessage{},
			cancelledCtx: true,
		},
		{
			name: "given_multiple_messages_when_transform_successfully_then_event_time_changed",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				message := input.Message
				message.EventTime = testEventTime
				output = append(output, &message)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
					},
					ReadOffset: testReadOffset2,
					Watermark:  testWatermark2,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
					},
					ReadOffset: testReadOffset3,
					Watermark:  testWatermark3,
				},
			},
			expectOutput: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset2,
					Watermark:  testWatermark2,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset3,
					Watermark:  testWatermark3,
				},
			},
		}, {
			name: "given_one_message_when_transform_to_multiple_messages_then_messages_share_same_watermark_and_read_offset",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				m1, m2, m3 := input.Message, input.Message, input.Message
				m1.EventTime, m2.EventTime, m3.EventTime = testEventTime, testEventTime, testEventTime
				output = append(output, &m1, &m2, &m3)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
			},
			expectOutput: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelledCtx {
				cancel()
			}

			transformer := New(tt.mapper, logging.NewLogger())
			got := transformer.Transform(ctx, tt.input)
			assert.Equal(t, len(tt.expectOutput), len(got))
			for i, v := range tt.expectOutput {
				assert.Equalf(t, v.Message, got[i].Message, "Transform() got = %v, want %v", got[i].Message, v.Message)
				assert.Equalf(t, v.Watermark, got[i].Watermark, "Transform() got watermark = %v, want %v", got[i].Watermark, v.Watermark)
				expectedOffset, err := v.ReadOffset.Sequence()
				assert.NoError(t, err)
				actualOffset, err := got[i].ReadOffset.Sequence()
				assert.NoError(t, err)
				assert.Equalf(t, expectedOffset, actualOffset, "Transform() got read offset = %v, want %v", actualOffset, expectedOffset)
			}
		})
	}
}

func TestTransformerNeverReturnPartialData(t *testing.T) {
	tests := []struct {
		name         string
		mapper       applier.MapApplier
		input        []*isb.ReadMessage
		expectOutput []*isb.ReadMessage
	}{
		{
			name: "given_no_error_then_return_full_result",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				message := input.Message
				_, err = strconv.Atoi(string(input.Message.Payload))
				message.EventTime = testEventTime
				output = append(output, &message)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
						Body: isb.Body{
							Payload: []byte("1"),
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
						Body: isb.Body{
							Payload: []byte("2"),
						},
					},
					ReadOffset: testReadOffset2,
					Watermark:  testWatermark2,
				},
			},
			expectOutput: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
						Body: isb.Body{
							Payload: []byte("1"),
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: testEventTime,
							},
						},
						Body: isb.Body{
							Payload: []byte("2"),
						},
					},
					ReadOffset: testReadOffset2,
					Watermark:  testWatermark2,
				},
			},
		},
		{
			name: "given_one_of_the_input_messages_failed_being_transformed_then_return_empty_result",
			mapper: applier.ApplyMapFunc(func(ctx context.Context, input *isb.ReadMessage) (output []*isb.Message, err error) {
				message := input.Message
				_, err = strconv.Atoi(string(input.Message.Payload))
				message.EventTime = testEventTime
				output = append(output, &message)
				return
			}),
			input: []*isb.ReadMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
						Body: isb.Body{
							Payload: []byte("1"),
						},
					},
					ReadOffset: testReadOffset,
					Watermark:  testWatermark,
				},
				{
					Message: isb.Message{
						Header: isb.Header{
							PaneInfo: isb.PaneInfo{
								EventTime: time.Now(),
							},
						},
						Body: isb.Body{
							// Mimic error scenario as "ab" can't be cast to an integer.
							Payload: []byte("ab"),
						},
					},
					ReadOffset: testReadOffset2,
					Watermark:  testWatermark2,
				},
			},
			expectOutput: []*isb.ReadMessage{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// If ApplyMap throws an error, let it retry for 2 times before canceling context.
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
			defer cancel()

			transformer := New(tt.mapper, logging.NewLogger())
			got := transformer.Transform(ctx, tt.input)
			assert.Equal(t, len(tt.expectOutput), len(got))
			for i, v := range tt.expectOutput {
				assert.Equalf(t, v.Message, got[i].Message, "Transform() got = %v, want %v", got[i].Message, v.Message)
				assert.Equalf(t, v.Watermark, got[i].Watermark, "Transform() got watermark = %v, want %v", got[i].Watermark, v.Watermark)
				expectedOffset, err := v.ReadOffset.Sequence()
				assert.NoError(t, err)
				actualOffset, err := got[i].ReadOffset.Sequence()
				assert.NoError(t, err)
				assert.Equalf(t, expectedOffset, actualOffset, "Transform() got read offset = %v, want %v", actualOffset, expectedOffset)
			}
		})
	}
}

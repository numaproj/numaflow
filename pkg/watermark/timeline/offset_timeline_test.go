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

package timeline

import (
	"context"
	"strconv"
	"testing"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/stretchr/testify/assert"
)

func TestTimeline_GetEventTime(t *testing.T) {
	var (
		ctx            = context.Background()
		emptyTimeline  = NewOffsetTimeline(ctx, 5)
		testTimeline   = NewOffsetTimeline(ctx, 10)
		testwatermarks = []wmb.WMB{
			{Watermark: 10, Offset: 9},
			{Watermark: 12, Offset: 10},
			{Watermark: 12, Offset: 20},
			{Watermark: 13, Offset: 21},
			{Watermark: 15, Offset: 24},
			{Watermark: 20, Offset: 26},
			{Watermark: 23, Offset: 27},
			{Watermark: 28, Offset: 30},
			{Watermark: 29, Offset: 35},
			{Watermark: 32, Offset: 36},
		}
	)

	for _, watermark := range testwatermarks {
		testTimeline.Put(watermark)
	}

	type args struct {
		timeline    *OffsetTimeline
		inputOffset int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "offset_0",
			args: args{
				timeline:    emptyTimeline,
				inputOffset: 0,
			},
			want: -1,
		},
		{
			name: "offset_7",
			args: args{
				timeline:    testTimeline,
				inputOffset: 7,
			},
			want: -1,
		},
		{
			name: "offset_9",
			args: args{
				timeline:    testTimeline,
				inputOffset: 9,
			},
			want: -1,
		},
		{
			name: "offset_13",
			args: args{
				timeline:    testTimeline,
				inputOffset: 13,
			},
			want: 10,
		},
		{
			name: "offset_24",
			args: args{
				timeline:    testTimeline,
				inputOffset: 24,
			},
			want: 13,
		},
		{
			name: "offset_28",
			args: args{
				timeline:    testTimeline,
				inputOffset: 28,
			},
			want: 23,
		},
		{
			name: "offset_30",
			args: args{
				timeline:    testTimeline,
				inputOffset: 30,
			},
			want: 23,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			if got := tt.args.timeline.GetEventTime(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.inputOffset, 10) })); got != tt.want {
				t1.Errorf("GetEventTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffsetTimeline_GetOffset(t *testing.T) {
	var (
		ctx            = context.Background()
		testTimeline   = NewOffsetTimeline(ctx, 10)
		testwatermarks = []wmb.WMB{
			{Watermark: 10, Offset: 9},
			{Watermark: 12, Offset: 20},
			{Watermark: 13, Offset: 21},
			{Watermark: 15, Offset: 24},
			{Watermark: 20, Offset: 26},
			{Watermark: 23, Offset: 27},
			{Watermark: 28, Offset: 30},
			{Watermark: 29, Offset: 35},
			{Watermark: 32, Offset: 36},
		}
	)

	for _, watermark := range testwatermarks {
		testTimeline.Put(watermark)
	}

	type args struct {
		timeline       *OffsetTimeline
		inputEventTime int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "eventTime_35",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 35,
			},
			want: 36,
		},
		{
			name: "eventTime_32",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 32,
			},
			want: 36,
		},
		{
			name: "eventTime_31",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 31,
			},
			want: 35,
		},
		{
			name: "eventTime_29",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 29,
			},
			want: 35,
		},
		{
			name: "eventTime_16",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 16,
			},
			want: 24,
		},
		{
			name: "eventTime_10",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 10,
			},
			want: 9,
		},
		{
			name: "eventTime_6",
			args: args{
				timeline:       testTimeline,
				inputEventTime: 6,
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			if got := tt.args.timeline.GetOffset(tt.args.inputEventTime); got != tt.want {
				t1.Errorf("GetOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffsetTimeline_PutIdle(t *testing.T) {
	var (
		ctx          = context.Background()
		testTimeline = NewOffsetTimeline(ctx, 10)
		setUps       = []wmb.WMB{
			{Idle: false, Watermark: 10, Offset: 9},
			{Idle: false, Watermark: 12, Offset: 20},
			{Idle: false, Watermark: 13, Offset: 21},
			{Idle: false, Watermark: 15, Offset: 24},
			{Idle: false, Watermark: 15, Offset: 25}, // will overwrite the previous one
			{Idle: false, Watermark: 20, Offset: 26},
			{Idle: false, Watermark: 23, Offset: 27},
			{Idle: false, Watermark: 28, Offset: 30},
			{Idle: false, Watermark: 29, Offset: 35},
			{Idle: false, Watermark: 32, Offset: 36},
		}
	)

	for _, watermark := range setUps {
		testTimeline.Put(watermark)
		assert.Equal(t, watermark, testTimeline.GetHeadWMB())
		assert.Equal(t, watermark.Watermark, testTimeline.GetHeadWatermark(), watermark.Watermark)
		assert.Equal(t, watermark.Offset, testTimeline.GetHeadOffset())
	}
	assert.Equal(t, "[32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20] -> [10:9] -> [-1:-1]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 33, Offset: 37}) // insert
	assert.Equal(t, "[IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20] -> [10:9]", testTimeline.Dump())

	testTimeline.Put(wmb.WMB{Idle: false, Watermark: 34, Offset: 38}) // insert
	assert.Equal(t, "[34:38] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 34, Offset: 39}) // same watermark, update
	assert.Equal(t, "[IDLE 34:39] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 36, Offset: 39}) // same offset, update
	assert.Equal(t, "[IDLE 36:39] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 36, Offset: 38}) // smaller offset, ignore
	assert.Equal(t, "[IDLE 36:39] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 35, Offset: 39}) // smaller watermark, ignore
	assert.Equal(t, "[IDLE 36:39] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21] -> [12:20]", testTimeline.Dump())

	testTimeline.PutIdle(wmb.WMB{Idle: true, Watermark: 37, Offset: 40}) // larger offset, insert
	assert.Equal(t, "[IDLE 37:40] -> [IDLE 36:39] -> [IDLE 33:37] -> [32:36] -> [29:35] -> [28:30] -> [23:27] -> [20:26] -> [15:25] -> [13:21]", testTimeline.Dump())

}

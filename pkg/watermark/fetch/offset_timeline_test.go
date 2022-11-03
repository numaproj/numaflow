//go:build isb_jetstream

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

package fetch

import (
	"context"
	"strconv"
	"testing"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestTimeline_GetEventTime(t1 *testing.T) {
	var (
		ctx            = context.Background()
		emptyTimeline  = NewOffsetTimeline(ctx, 5)
		testTimeline   = NewOffsetTimeline(ctx, 10)
		testwatermarks = []OffsetWatermark{
			{watermark: 10, offset: 9},
			{watermark: 12, offset: 10},
			{watermark: 12, offset: 20},
			{watermark: 13, offset: 21},
			{watermark: 15, offset: 24},
			{watermark: 20, offset: 26},
			{watermark: 23, offset: 27},
			{watermark: 28, offset: 30},
			{watermark: 29, offset: 35},
			{watermark: 32, offset: 36},
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
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.args.timeline.GetEventTime(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.inputOffset, 10) })); got != tt.want {
				t1.Errorf("GetEventTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffsetTimeline_GetOffset(t1 *testing.T) {
	var (
		ctx            = context.Background()
		testTimeline   = NewOffsetTimeline(ctx, 10)
		testwatermarks = []OffsetWatermark{
			{watermark: 10, offset: 9},
			{watermark: 12, offset: 20},
			{watermark: 13, offset: 21},
			{watermark: 15, offset: 24},
			{watermark: 20, offset: 26},
			{watermark: 23, offset: 27},
			{watermark: 28, offset: 30},
			{watermark: 29, offset: 35},
			{watermark: 32, offset: 36},
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
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.args.timeline.GetOffset(tt.args.inputEventTime); got != tt.want {
				t1.Errorf("GetOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

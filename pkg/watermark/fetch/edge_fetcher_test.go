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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"github.com/numaproj/numaflow/pkg/isb"
	natsclass "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func TestBuffer_updateWatermarkWithOnePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We don't really need watcher because we manually call the `put` function and the `addProcessor` function
	// so use no op watcher for testing
	wmStore, _ := store.BuildNoOpWatermarkStore()
	pm := newProcessorManager(ctx, wmStore, 1)
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, 1)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, 1)
		testPod2     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, 1)
		pod0Timeline = []wmb.WMB{
			{Watermark: 11, Offset: 9, Partition: 0},
			{Watermark: 12, Offset: 20, Partition: 0},
			{Watermark: 13, Offset: 21, Partition: 0},
			{Watermark: 14, Offset: 22, Partition: 0},
			{Watermark: 17, Offset: 28, Partition: 0},
		}
		pod1Timeline = []wmb.WMB{
			{Watermark: 8, Offset: 13, Partition: 0},
			{Watermark: 9, Offset: 16, Partition: 0},
			{Watermark: 10, Offset: 18, Partition: 0},
			{Watermark: 17, Offset: 26, Partition: 0},
		}
		pod2Timeline = []wmb.WMB{
			{Watermark: 10, Offset: 14, Partition: 0},
			{Watermark: 12, Offset: 17, Partition: 0},
			{Watermark: 14, Offset: 19, Partition: 0},
			{Watermark: 17, Offset: 24, Partition: 0},
		}
	)

	for _, watermark := range pod0Timeline {
		for _, tl := range testPod0.GetOffsetTimelines() {
			tl.Put(watermark)
		}
	}
	for _, watermark := range pod1Timeline {
		for _, tl := range testPod1.GetOffsetTimelines() {
			tl.Put(watermark)
		}
	}
	for _, watermark := range pod2Timeline {
		for _, tl := range testPod2.GetOffsetTimelines() {
			tl.Put(watermark)
		}
	}
	pm.addProcessor("testPod0", testPod0)
	pm.addProcessor("testPod1", testPod1)
	pm.addProcessor("testPod2", testPod2)

	type args struct {
		offset int64
	}
	tests := []struct {
		name             string
		processorManager *processorManager
		args             args
		want             int64
	}{
		{
			name:             "offset_9",
			processorManager: pm,
			args:             args{9},
			want:             -1,
		},
		{
			name:             "offset_15",
			processorManager: pm,
			args:             args{15},
			want:             8,
		},
		{
			name:             "offset_18",
			processorManager: pm,
			args:             args{18},
			want:             9,
		},
		{
			name:             "offset_22",
			processorManager: pm,
			args:             args{22},
			want:             10,
		},
		{
			name:             "offset_23",
			processorManager: pm,
			args:             args{23},
			want:             10,
		},
		{
			name:             "offset_28",
			processorManager: pm,
			args:             args{28},
			want:             14,
		},
		{
			name:             "offset_29",
			processorManager: pm,
			args:             args{29},
			want:             17,
		},
	}
	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastProcessed []int64
			for i := 0; i < 1; i++ {
				lastProcessed = append(lastProcessed, -1)
			}
			b := &edgeFetcher{
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
				lastProcessedWm:  lastProcessed,
				opts:             defaultOptions(),
			}
			if got := b.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) }), 0); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("ComputeWatermark() = %v, want %v", got, wmb.Watermark(time.UnixMilli(tt.want)))
			}
			// this will always be 17 because the timeline has been populated ahead of time
			// ComputeHeadWatermark is only used in UI and test
			assert.Equal(t, time.Time(b.ComputeHeadWatermark(0)).In(location), time.UnixMilli(17).In(location))
		})
	}
}

func TestBuffer_updateWatermarkWithMultiplePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We don't really need watcher because we manually call the `put` function and the `addProcessor` function
	// so use no op watcher for testing
	wmStore, _ := store.BuildNoOpWatermarkStore()
	partitionCount := int32(3)
	pm := newProcessorManager(ctx, wmStore, partitionCount)
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline = []wmb.WMB{
			{Watermark: 11, Offset: 9, Partition: 0},
			{Watermark: 12, Offset: 20, Partition: 1},
			{Watermark: 13, Offset: 21, Partition: 2},
			{Watermark: 14, Offset: 22, Partition: 0},
			{Watermark: 17, Offset: 28, Partition: 1},
			{Watermark: 25, Offset: 30, Partition: 2},
			{Watermark: 26, Offset: 31, Partition: 0},
			{Watermark: 27, Offset: 32, Partition: 1},
		}
		pod1Timeline = []wmb.WMB{
			{Watermark: 8, Offset: 13, Partition: 0},
			{Watermark: 9, Offset: 16, Partition: 1},
			{Watermark: 10, Offset: 18, Partition: 2},
			{Watermark: 17, Offset: 26, Partition: 0},
			{Watermark: 27, Offset: 29, Partition: 1},
			{Watermark: 28, Offset: 33, Partition: 2},
			{Watermark: 29, Offset: 34, Partition: 0},
		}
		pod2Timeline = []wmb.WMB{
			{Watermark: 10, Offset: 14, Partition: 0},
			{Watermark: 12, Offset: 17, Partition: 1},
			{Watermark: 14, Offset: 19, Partition: 2},
			{Watermark: 17, Offset: 24, Partition: 0},
			{Watermark: 25, Offset: 35, Partition: 1},
			{Watermark: 26, Offset: 36, Partition: 2},
			{Watermark: 27, Offset: 37, Partition: 0},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	pm.addProcessor("testPod0", testPod0)
	pm.addProcessor("testPod1", testPod1)
	pm.addProcessor("testPod2", testPod2)

	type args struct {
		offset int64
	}
	tests := []struct {
		name            string
		pm              *processorManager
		args            args
		want            int64
		partitionIdx    int32
		lastProcessedWm []int64
	}{
		{
			name:            "offset_9",
			pm:              pm,
			args:            args{9},
			want:            -1,
			partitionIdx:    0,
			lastProcessedWm: []int64{-1, -1, -1},
		},
		{
			name:            "offset_15",
			pm:              pm,
			args:            args{15},
			want:            -1,
			partitionIdx:    1,
			lastProcessedWm: []int64{-1, -1, -1},
		},
		{
			name:            "offset_18",
			pm:              pm,
			args:            args{18},
			partitionIdx:    0,
			lastProcessedWm: []int64{6, 8, 8},
			want:            8,
		},
		{
			name:            "offset_22",
			pm:              pm,
			args:            args{22},
			partitionIdx:    2,
			lastProcessedWm: []int64{6, 5, 6},
			want:            5,
		},
		{
			name:            "offset_23",
			pm:              pm,
			args:            args{23},
			partitionIdx:    0,
			lastProcessedWm: []int64{7, 8, 7},
			want:            7,
		},
		{
			name:            "offset_28",
			pm:              pm,
			args:            args{28},
			partitionIdx:    1,
			lastProcessedWm: []int64{8, 9, 8},
			want:            8,
		},
		{
			name:            "offset_29",
			pm:              pm,
			args:            args{29},
			partitionIdx:    2,
			lastProcessedWm: []int64{10, 10, 9},
			want:            10,
		},
		{
			name:            "offset_31",
			pm:              pm,
			args:            args{31},
			partitionIdx:    0,
			lastProcessedWm: []int64{13, 14, 14},
			want:            14,
		},
		{
			name:            "offset_35",
			pm:              pm,
			args:            args{40},
			partitionIdx:    1,
			lastProcessedWm: []int64{25, 26, 27},
			want:            25,
		},
	}
	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastProcessed := make([]int64, 3)
			for i := 0; i < 3; i++ {
				lastProcessed[i] = wmb.InitialWatermark.UnixMilli()
			}
			b := &edgeFetcher{
				processorManager: tt.pm,
				log:              zaptest.NewLogger(t).Sugar(),
				lastProcessedWm:  tt.lastProcessedWm,
				opts:             defaultOptions(),
			}
			_ = b.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) }), tt.partitionIdx)
			if got := b.getWatermark(); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("ComputeWatermark() = %v, want %v", got, wmb.Watermark(time.UnixMilli(tt.want)))
			}
			// this will always be 27 because the timeline has been populated ahead of time
			// ComputeHeadWatermark is only used in UI and test
			assert.Equal(t, time.Time(b.ComputeHeadWatermark(0)).In(location), time.UnixMilli(26).In(location))
		})
	}
}

func Test_edgeFetcher_ComputeHeadWatermark(t *testing.T) {
	var (
		partitionCount    = int32(2)
		ctx, cancel       = context.WithCancel(context.Background())
		wmStore, _        = store.BuildNoOpWatermarkStore()
		processorManager1 = newProcessorManager(ctx, wmStore, partitionCount)
		processorManager2 = newProcessorManager(ctx, wmStore, partitionCount)
	)
	defer cancel()

	computeHeadWMTest1(ctx, processorManager1)
	computeHeadWMTest2(ctx, processorManager2)

	tests := []struct {
		name             string
		processorManager *processorManager
		want             int64
	}{
		{
			name:             "all pods idle and get an idle WMB",
			processorManager: processorManager1,
			want:             14,
		},
		{
			name:             "some pods idle and skip an idle WMB",
			processorManager: processorManager2,
			want:             17,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &edgeFetcher{
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
				opts:             defaultOptions(),
			}
			assert.Equalf(t, tt.want, e.ComputeHeadWatermark(0).UnixMilli(), "ComputeHeadWatermark()")
		})
	}
}

func computeHeadWMTest1(ctx context.Context, processorManager1 *processorManager) {
	var (
		partitionCount = int32(2)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline   = []wmb.WMB{
			{
				Idle:      true,
				Offset:    28,
				Watermark: 14,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    27,
				Watermark: 13,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 15,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    25,
				Watermark: 14,
				Partition: 1,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    23,
				Watermark: 16,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager1.addProcessor("testPod0", testPod0)
	processorManager1.addProcessor("testPod1", testPod1)
	processorManager1.addProcessor("testPod2", testPod2)
}

func computeHeadWMTest2(ctx context.Context, processorManager2 *processorManager) {
	var (
		partitionCount = int32(2)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline   = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    29,
				Watermark: 16,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    23,
				Watermark: 18,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager2.addProcessor("testPod0", testPod0)
	processorManager2.addProcessor("testPod1", testPod1)
	processorManager2.addProcessor("testPod2", testPod2)
}

func Test_edgeFetcher_updateHeadIdleWMB(t *testing.T) {
	var (
		partitionCount    = int32(3)
		ctx, cancel       = context.WithCancel(context.Background())
		wmStore, _        = store.BuildNoOpWatermarkStore()
		processorManager1 = newProcessorManager(ctx, wmStore, partitionCount)
		processorManager2 = newProcessorManager(ctx, wmStore, partitionCount)
		processorManager3 = newProcessorManager(ctx, wmStore, partitionCount)
		processorManager4 = newProcessorManager(ctx, wmStore, partitionCount)
	)
	defer cancel()

	updateHeadIdleWMBTest1(ctx, processorManager1)
	updateHeadIdleWMBTest2(ctx, processorManager2)
	updateHeadIdleWMBTest3(ctx, processorManager3)
	updateHeadIdleWMBTest4(ctx, processorManager4)

	tests := []struct {
		name             string
		processorManager *processorManager
		want             wmb.WMB
	}{
		{
			name:             "all pods idle and get an idle WMB",
			processorManager: processorManager1,
			want: wmb.WMB{
				Idle:      true,
				Offset:    22,
				Watermark: 17,
				Partition: 0,
			},
		},
		{
			name:             "some pods idle and skip an idle WMB",
			processorManager: processorManager2,
			want:             wmb.WMB{},
		},
		{
			name:             "all pods not idle and skip an idle WMB",
			processorManager: processorManager3,
			want:             wmb.WMB{},
		},
		{
			name:             "all pods empty timeline",
			processorManager: processorManager4,
			want:             wmb.WMB{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastProcessedWm = make([]int64, partitionCount)
			for i := 0; i < int(partitionCount); i++ {
				lastProcessedWm[i] = 100
			}
			e := &edgeFetcher{
				processorManager: tt.processorManager,
				lastProcessedWm:  lastProcessedWm,
				log:              zaptest.NewLogger(t).Sugar(),
				opts:             defaultOptions(),
			}
			assert.Equalf(t, tt.want, e.updateHeadIdleWMB(0), "updateHeadIdleWMB()")
		})
	}
}

func updateHeadIdleWMBTest1(ctx context.Context, processorManager1 *processorManager) {
	var (
		partitionCount = int32(3)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline   = []wmb.WMB{
			{
				Idle:      true,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    26,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    23,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    22,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    21,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    20,
				Watermark: 15,
				Partition: 2,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager1.addProcessor("testPod0", testPod0)
	processorManager1.addProcessor("testPod1", testPod1)
	processorManager1.addProcessor("testPod2", testPod2)
}

func updateHeadIdleWMBTest2(ctx context.Context, processorManager2 *processorManager) {
	var (
		partitionCount = int32(3)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline   = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    26,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    23,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    22,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    21,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      true,
				Offset:    20,
				Watermark: 15,
				Partition: 2,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager2.addProcessor("testPod0", testPod0)
	processorManager2.addProcessor("testPod1", testPod1)
	processorManager2.addProcessor("testPod2", testPod2)
}

func updateHeadIdleWMBTest3(ctx context.Context, processorManager3 *processorManager) {
	var (
		partitionCount = int32(3)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
		pod0Timeline   = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      false,
				Offset:    26,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      false,
				Offset:    23,
				Watermark: 15,
				Partition: 2,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    22,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    21,
				Watermark: 16,
				Partition: 1,
			},
			{
				Idle:      false,
				Offset:    20,
				Watermark: 15,
				Partition: 2,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager3.addProcessor("testPod0", testPod0)
	processorManager3.addProcessor("testPod1", testPod1)
	processorManager3.addProcessor("testPod2", testPod2)
}

func updateHeadIdleWMBTest4(ctx context.Context, processorManager4 *processorManager) {
	var (
		partitionCount = int32(3)
		testPod0       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, partitionCount)
		testPod1       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, partitionCount)
		testPod2       = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod3"), 5, partitionCount)
	)
	processorManager4.addProcessor("testPod0", testPod0)
	processorManager4.addProcessor("testPod1", testPod1)
	processorManager4.addProcessor("testPod2", testPod2)
}

// end to end test for fetcher using inmem store
func TestFetcherWithSameOTBucket_InMem(t *testing.T) {
	var (
		err        error
		keyspace         = "fetcherTest"
		epoch      int64 = 1651161600000
		testOffset int64 = 100
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	wmStore, err := store.BuildInmemWatermarkStore(ctx, keyspace)
	assert.NoError(t, err)
	defer wmStore.Close()

	epoch += 60000

	var fetcher = NewEdgeFetcher(ctx, wmStore, 1)

	var heartBeatManagerMap = make(map[string]*heartBeatManager)
	heartBeatManagerMap["p1"] = manageHeartbeat(ctx, "p1", wmStore.HeartbeatStore())
	heartBeatManagerMap["p2"] = manageHeartbeat(ctx, "p2", wmStore.HeartbeatStore())

	heartBeatManagerMap["p1"].start()
	heartBeatManagerMap["p2"].start()

	allProcessors := fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	otValueByte, err := otValueToBytes(testOffset, epoch, false, 0)
	assert.NoError(t, err)
	err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	otValueByte, err = otValueToBytes(testOffset+5, epoch, false, 0)
	assert.NoError(t, err)
	err = wmStore.OffsetTimelineStore().PutKV(ctx, "p2", otValueByte)
	assert.NoError(t, err)

	for allProcessors["p1"].GetOffsetTimelines()[0].GetHeadOffset() != 100 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 head offset to be 100: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	heartBeatManagerMap["p1"].stop()

	// "p1" status becomes deleted since we stopped the heartbeat
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }), 0)
	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=103
	// because "p1" offsetTimeline's head offset=100, which is < inputOffset 103
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }), 0)
	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 1, len(allProcessors))
	assert.True(t, allProcessors["p2"].IsActive())

	heartBeatManagerMap["p1"].start()

	// wait until p1 becomes active
	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	for !allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }), 0)
	p1 := fetcher.processorManager.getProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.GetOffsetTimelines())
	assert.Equal(t, int64(-1), p1.GetOffsetTimelines()[0].GetHeadOffset())

	// publish a new watermark 101
	otValueByte, err = otValueToBytes(testOffset+1, epoch, false, 0)
	assert.NoError(t, err)
	err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	heartBeatManagerMap["p1"].stop()

	// "p1" becomes inactive since we stopped the heartbeat
	for allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	heartBeatManagerMap["p1"].start()

	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// added 101 in the previous steps for p1, so the head should be 101 after resume
	assert.Equal(t, int64(101), p1.GetOffsetTimelines()[0].GetHeadOffset())
	heartBeatManagerMap["p1"].stop()
	heartBeatManagerMap["p2"].stop()

	for allProcessors["p1"].IsInactive() && allProcessors["p2"].IsInactive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 0 processors to be active, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
}

// end to end test for fetcher with same ot bucket
func TestFetcherWithSameOTBucketWithSinglePartition(t *testing.T) {
	var (
		keyspace         = "fetcherTestSinglePartition"
		epoch      int64 = 1651161600000
		testOffset int64 = 100
	)

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// connect to NATS
	nc := natsclass.NewTestClientWithServer(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create heartbeat bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_PROCESSORS",
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_PROCESSORS") }()
	assert.NoError(t, err)

	// create offset timeline bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_OT",
		Description:  "",
		MaxValueSize: 0,
		History:      2,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT") }()
	assert.NoError(t, err)

	defaultJetStreamClient := natsclass.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()

	// create hbStore
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	defer hbStore.Close()

	// create otStore
	otStore, err := jetstream.NewKVJetStreamKVStore(ctx, keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	defer otStore.Close()

	// create wm stores
	wmStore, err := store.BuildJetStreamWatermarkStore(ctx, keyspace, defaultJetStreamClient)
	assert.NoError(t, err)
	fetcher := NewEdgeFetcher(ctx, wmStore, 1)

	var heartBeatManagerMap = make(map[string]*heartBeatManager)
	heartBeatManagerMap["p1"] = manageHeartbeat(ctx, "p1", hbStore)
	heartBeatManagerMap["p2"] = manageHeartbeat(ctx, "p2", hbStore)

	// start the heartbeats for p1 and p2
	heartBeatManagerMap["p1"].start()
	heartBeatManagerMap["p2"].start()

	allProcessors := fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	for !allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active, got %t: %s", allProcessors["p1"].IsActive(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// put values into otStore
	// this first entry should not be in the offset timeline because we set the wmb bucket history to 2
	otValueByte, err := otValueToBytes(testOffset, epoch+100, false, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	otValueByte, err = otValueToBytes(testOffset+1, epoch+200, false, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	otValueByte, err = otValueToBytes(testOffset+2, epoch+300, false, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	epoch += 60000

	otValueByte, err = otValueToBytes(testOffset+5, epoch+500, false, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByte)
	assert.NoError(t, err)

	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[1651161600300:102] -> [1651161600200:101] -> [1651161600100:100] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161600300:102] -> [1651161600200:101] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	heartBeatManagerMap["p1"].stop()
	// "p1" will be deleted since we stop the heartbeat

	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }), 0)
	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=103
	// because "p1" offsetTimeline's head offset=102, which is < inputOffset 103
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }), 0)
	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 1, len(allProcessors))
	assert.True(t, allProcessors["p2"].IsActive())

	heartBeatManagerMap["p1"].start()

	// wait until p1 becomes active
	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }), 0)
	p1 := fetcher.processorManager.getProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.GetOffsetTimelines())
	assert.Equal(t, int64(-1), p1.GetOffsetTimelines()[0].GetHeadOffset())

	// publish a new watermark 103
	otValueByte, err = otValueToBytes(testOffset+3, epoch+500, false, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	heartBeatManagerMap["p1"].resume()

	// "p1" becomes inactive after stopping the heartbeat
	for allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	heartBeatManagerMap["p1"].start()
	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// added 103 in the previous steps for p1, so the head should be 103 after resume
	assert.Equal(t, int64(103), p1.GetOffsetTimelines()[0].GetHeadOffset())

	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// publish an idle watermark: simulate reduce
	otValueByte, err = otValueToBytes(106, epoch+600, true, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// publish an idle watermark: simulate map
	otValueByte, err = otValueToBytes(107, epoch+700, true, 0)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> ..., got %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
	heartBeatManagerMap["p1"].stop()
	heartBeatManagerMap["p2"].stop()

	for allProcessors["p1"].IsInactive() && allProcessors["p2"].IsInactive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 0 processors to be active, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
}

// end to end test for fetcher with same ot bucket
func TestFetcherWithSameOTBucketWithMultiplePartition(t *testing.T) {
	var (
		keyspace         = "fetcherTestMultiPartition"
		epoch      int64 = 1651161600000
		testOffset int64 = 100
	)

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// connect to NATS
	nc := natsclass.NewTestClientWithServer(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create heartbeat bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_PROCESSORS",
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_PROCESSORS") }()
	assert.NoError(t, err)

	// create offset timeline bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_OT",
		Description:  "",
		MaxValueSize: 0,
		History:      10,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT") }()
	assert.NoError(t, err)

	defaultJetStreamClient := natsclass.NewTestClientWithServer(t, s)
	defer defaultJetStreamClient.Close()

	// create hbStore
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	defer hbStore.Close()

	// create otStore
	otStore, err := jetstream.NewKVJetStreamKVStore(ctx, keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	defer otStore.Close()

	// create watchers for heartbeat and offset timeline
	wmStore, err := store.BuildJetStreamWatermarkStore(ctx, keyspace, defaultJetStreamClient)
	assert.NoError(t, err)
	fetcher := NewEdgeFetcher(ctx, wmStore, 3)

	var heartBeatManagerMap = make(map[string]*heartBeatManager)
	heartBeatManagerMap["p1"] = manageHeartbeat(ctx, "p1", hbStore)
	heartBeatManagerMap["p2"] = manageHeartbeat(ctx, "p2", hbStore)

	// start the heartbeats for p1 and p2
	heartBeatManagerMap["p1"].start()
	heartBeatManagerMap["p2"].start()

	allProcessors := fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// put values into otStore
	otValueByteOne, err := otValueToBytes(testOffset, epoch+100, false, 0)
	assert.NoError(t, err)
	otValueByteTwo, err := otValueToBytes(testOffset, epoch+100, false, 1)
	assert.NoError(t, err)
	otValueByteThree, err := otValueToBytes(testOffset, epoch+100, false, 2)
	assert.NoError(t, err)

	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	otValueByteOne, err = otValueToBytes(testOffset+1, epoch+200, false, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(testOffset+1, epoch+200, false, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(testOffset+1, epoch+200, false, 2)
	assert.NoError(t, err)

	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	otValueByteOne, err = otValueToBytes(testOffset+2, epoch+300, false, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(testOffset+2, epoch+300, false, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(testOffset+2, epoch+300, false, 2)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	epoch += 60000

	otValueByteOne, err = otValueToBytes(testOffset+5, epoch+500, false, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(testOffset+5, epoch+500, false, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(testOffset+5, epoch+500, false, 2)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByteThree)
	assert.NoError(t, err)

	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[1651161600300:102] -> [1651161600200:101] -> [1651161600100:100] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[1].Dump() != "[1651161600300:102] -> [1651161600200:101] -> [1651161600100:100] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[2].Dump() != "[1651161600300:102] -> [1651161600200:101] -> [1651161600100:100] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timelines [1651161600300:102] -> [1651161600200:101] -> [1651161600100:100]..., [1651161600300:102] -> [1651161600200:101] -> [1651161600100:100]..., "+
					"[1651161600300:102] -> [1651161600200:101] -> [1651161600100:100]... got %s %s %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), allProcessors["p1"].GetOffsetTimelines()[1].Dump(), allProcessors["p1"].GetOffsetTimelines()[2].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	heartBeatManagerMap["p1"].stop()
	// "p1" is deleted since we stop the heartbeat
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }), 0)
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }), 1)
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }), 2)

	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=103
	// because "p1" offsetTimeline's head offset=102, which is < inputOffset 103
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }), 0)
	allProcessors = fetcher.processorManager.getAllProcessors()
	assert.Equal(t, 1, len(allProcessors))
	assert.True(t, allProcessors["p2"].IsActive())

	heartBeatManagerMap["p1"].start()

	// wait until p1 becomes active
	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = fetcher.updateWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }), 0)
	p1 := fetcher.processorManager.getProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.GetOffsetTimelines())
	assert.Equal(t, int64(-1), p1.GetOffsetTimelines()[0].GetHeadOffset())

	// publish a new watermark 103
	otValueByteOne, err = otValueToBytes(testOffset+3, epoch+500, false, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(testOffset+3, epoch+500, false, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(testOffset+3, epoch+500, false, 2)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	heartBeatManagerMap["p1"].resume()
	// "p1" is inactive since we resume the heartbeat
	for allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	heartBeatManagerMap["p1"].start()

	allProcessors = fetcher.processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// added 103 in the previous steps for p1, so the head should be 103 after resume
	assert.Equal(t, int64(103), p1.GetOffsetTimelines()[0].GetHeadOffset())

	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[1].Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[2].Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timelines [1651161660500:103] -> [-1:-1] -> [-1:-1]..., [1651161660500:103] -> [-1:-1] -> [-1:-1]..., "+
					"[1651161660500:103] -> [-1:-1] -> [-1:-1]... got %s %s %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), allProcessors["p1"].GetOffsetTimelines()[1].Dump(), allProcessors["p1"].GetOffsetTimelines()[2].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// publish an idle watermark: simulate reduce
	otValueByteOne, err = otValueToBytes(106, epoch+600, true, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(106, epoch+600, true, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(106, epoch+600, true, 2)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[1].Dump() != "[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[2].Dump() != "[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {

		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timelines [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]..., [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]..., "+
					"[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]... got %s %s %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), allProcessors["p1"].GetOffsetTimelines()[1].Dump(), allProcessors["p1"].GetOffsetTimelines()[2].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}

	// publish an idle watermark: simulate map
	otValueByteOne, err = otValueToBytes(107, epoch+700, true, 0)
	assert.NoError(t, err)
	otValueByteTwo, err = otValueToBytes(107, epoch+700, true, 1)
	assert.NoError(t, err)
	otValueByteThree, err = otValueToBytes(107, epoch+700, true, 2)
	assert.NoError(t, err)

	err = otStore.PutKV(ctx, "p1", otValueByteOne)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteTwo)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByteThree)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimelines()[0].Dump() != "[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[1].Dump() != "[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" &&
		allProcessors["p1"].GetOffsetTimelines()[2].Dump() != "[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timelines [IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]..., [IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]..., "+
					"[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1]... got %s %s %s: %s", allProcessors["p1"].GetOffsetTimelines()[0].Dump(), allProcessors["p1"].GetOffsetTimelines()[1].Dump(), allProcessors["p1"].GetOffsetTimelines()[2].Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
	heartBeatManagerMap["p1"].stop()
	heartBeatManagerMap["p2"].stop()

	for allProcessors["p1"].IsInactive() && allProcessors["p2"].IsInactive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 0 processors to be active, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = fetcher.processorManager.getAllProcessors()
		}
	}
}

type heartBeatManager struct {
	heartBeatCh chan int
}

func (h *heartBeatManager) start() {
	h.heartBeatCh <- 1
}

func (h *heartBeatManager) stop() {
	h.heartBeatCh <- 0
}

func (h *heartBeatManager) resume() {
	h.heartBeatCh <- 2
}

func manageHeartbeat(ctx context.Context, entityName string, hbStore kvs.KVStorer) *heartBeatManager {
	hbManager := &heartBeatManager{
		heartBeatCh: make(chan int),
	}
	go func() {
		var start bool
		for {
			select {
			case <-ctx.Done():
				return
			case val := <-hbManager.heartBeatCh:
				if val == 0 {
					_ = hbStore.DeleteKey(ctx, entityName)
				}
				start = !start
			default:
				if start {
					_ = hbStore.PutKV(ctx, entityName, []byte(fmt.Sprintf("%d", time.Now().Unix())))
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	return hbManager
}

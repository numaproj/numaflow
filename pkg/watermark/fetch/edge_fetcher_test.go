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
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

func TestBuffer_GetWatermark(t *testing.T) {
	var ctx = context.Background()

	// We don't really need watcher because we manually call the `Put` function and the `addProcessor` function
	// so use no op watcher for testing
	hbWatcher := noop.NewKVOpWatch()
	otWatcher := noop.NewKVOpWatch()
	processorManager := NewProcessorManager(ctx, store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher))
	var (
		testPod0     = NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5, otWatcher)
		testPod1     = NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5, otWatcher)
		testPod2     = NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5, otWatcher)
		pod0Timeline = []OffsetWatermark{
			{watermark: 11, offset: 9},
			{watermark: 12, offset: 20},
			{watermark: 13, offset: 21},
			{watermark: 14, offset: 22},
			{watermark: 17, offset: 28},
		}
		pod1Timeline = []OffsetWatermark{
			{watermark: 8, offset: 13},
			{watermark: 9, offset: 16},
			{watermark: 10, offset: 18},
			{watermark: 17, offset: 26},
		}
		pod2Timeline = []OffsetWatermark{
			{watermark: 10, offset: 14},
			{watermark: 12, offset: 17},
			{watermark: 14, offset: 19},
			{watermark: 17, offset: 24},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.offsetTimeline.Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.offsetTimeline.Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.offsetTimeline.Put(watermark)
	}
	processorManager.addProcessor("testPod0", testPod0)
	processorManager.addProcessor("testPod1", testPod1)
	processorManager.addProcessor("testPod2", testPod2)

	type args struct {
		offset int64
	}
	tests := []struct {
		name             string
		processorManager *ProcessorManager
		args             args
		want             int64
	}{
		{
			name:             "offset_9",
			processorManager: processorManager,
			args:             args{9},
			want:             -1,
		},
		{
			name:             "offset_15",
			processorManager: processorManager,
			args:             args{15},
			want:             8,
		},
		{
			name:             "offset_18",
			processorManager: processorManager,
			args:             args{18},
			want:             9,
		},
		{
			name:             "offset_22",
			processorManager: processorManager,
			args:             args{22},
			want:             10,
		},
		{
			name:             "offset_23",
			processorManager: processorManager,
			args:             args{23},
			want:             10,
		},
		{
			name:             "offset_28",
			processorManager: processorManager,
			args:             args{28},
			want:             14,
		},
		{
			name:             "offset_29",
			processorManager: processorManager,
			args:             args{29},
			want:             17,
		},
	}
	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &edgeFetcher{
				ctx:              ctx,
				edgeName:         "testBuffer",
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
			}
			if got := b.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) })); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("GetWatermark() = %v, want %v", got, processor.Watermark(time.UnixMilli(tt.want)))
			}
			// this will always be 14 because the timeline has been populated ahead of time
			assert.Equal(t, time.Time(b.GetHeadWatermark()).In(location), time.UnixMilli(14).In(location))
		})
	}
}

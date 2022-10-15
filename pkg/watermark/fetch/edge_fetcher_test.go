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
			want:             time.Time{}.Unix(),
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
			if got := b.GetWatermark(isb.SimpleOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) })); time.Time(got).In(location) != time.Unix(tt.want, 0).In(location) {
				t.Errorf("GetWatermark() = %v, want %v", got, processor.Watermark(time.Unix(tt.want, 0)))
			}
			// this will always be 14 because the timeline has been populated ahead of time
			assert.Equal(t, time.Time(b.GetHeadWatermark()).In(location), time.Unix(14, 0).In(location))
		})
	}
}

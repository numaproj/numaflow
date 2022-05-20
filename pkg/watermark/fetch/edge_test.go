//go:build isb_jetstream

package fetch

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
)

func TestBuffer_GetWatermark(t *testing.T) {

	var (
		ctx = context.Background()
		// TODO: watcher should not be nil
		testPod0     = NewProcessor(ctx, processor.NewProcessorEntity("testPod1", "test"), 5, nil)
		testPod1     = NewProcessor(ctx, processor.NewProcessorEntity("testPod2", "test"), 5, nil)
		testPod2     = NewProcessor(ctx, processor.NewProcessorEntity("testPod3", "test"), 5, nil)
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
		testVertex = NewFromVertex(ctx, "testVertex", nil, nil)
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
	testVertex.AddProcessor("testPod0", testPod0)
	testVertex.AddProcessor("testPod1", testPod1)
	testVertex.AddProcessor("testPod2", testPod2)

	type args struct {
		offset int64
	}
	tests := []struct {
		name       string
		fromVertex *FromVertex
		args       args
		want       int64
	}{
		{
			name:       "offset_9",
			fromVertex: testVertex,
			args:       args{9},
			want:       time.Time{}.Unix(),
		},
		{
			name:       "offset_15",
			fromVertex: testVertex,
			args:       args{15},
			want:       8,
		},
		{
			name:       "offset_18",
			fromVertex: testVertex,
			args:       args{18},
			want:       9,
		},
		{
			name:       "offset_22",
			fromVertex: testVertex,
			args:       args{22},
			want:       10,
		},
		{
			name:       "offset_28",
			fromVertex: testVertex,
			args:       args{28},
			want:       14,
		},
		{
			name:       "offset_28",
			fromVertex: testVertex,
			args:       args{29},
			want:       17,
		},
	}
	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EdgeBuffer{
				ctx:        ctx,
				name:       "testBuffer",
				fromVertex: tt.fromVertex,
			}
			if got := b.GetWatermark(isb.SimpleOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) })); time.Time(got).In(location) != time.Unix(tt.want, 0).In(location) {
				t.Errorf("GetWatermark() = %v, want %v", got, processor.Watermark(time.Unix(tt.want, 0)))
			}
		})
	}
}

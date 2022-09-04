package sliding

import (
	"fmt"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
	"github.com/stretchr/testify/assert"
)

func TestSliding_AssignWindow(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")
	baseTime := time.Unix(1651129201, 0).In(loc)

	type fields struct {
		Length          time.Duration
		PeriodInSeconds int
	}

	tests := []struct {
		name      string
		fields    fields
		eventTime time.Time
		want      []*window.IntervalWindow
	}{
		{
			name: "2in_1min",
			fields: fields{
				Length:          120 * time.Second,
				PeriodInSeconds: 60,
			},
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129260, 0).In(loc),
				},
				{
					Start: time.Unix(1651129200-60, 0).In(loc),
					End:   time.Unix(1651129260-60, 0).In(loc),
				},
			},
		},
		{
			name: "2min-as-sec_1min",
			fields: fields{
				Length:          2 * time.Minute,
				PeriodInSeconds: 60,
			},
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+60, 0).In(loc),
				},
				{
					Start: time.Unix(1651129200-60, 0).In(loc),
					End:   time.Unix(1651129200+60-60, 0).In(loc),
				},
			},
		},
		{
			name: "4min_2min",
			fields: fields{
				Length:          4 * time.Minute,
				PeriodInSeconds: 120,
			},
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+120, 0).In(loc),
				},
				{
					Start: time.Unix(1651129200-120, 0).In(loc),
					End:   time.Unix(1651129200+120-120, 0).In(loc),
				},
			},
		},
		{
			name: "4min_4min",
			fields: fields{
				Length:          4 * time.Minute,
				PeriodInSeconds: 240,
			},
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+240, 0).In(loc),
				},
			},
		},
		{
			name: "4min_5min",
			fields: fields{
				Length:          4 * time.Minute,
				PeriodInSeconds: 300,
			},
			eventTime: baseTime,
			want:      []*window.IntervalWindow{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSliding(tt.fields.Length, tt.fields.PeriodInSeconds)
			got := s.AssignWindow(tt.eventTime)
			equal, err := windowIntervalEqual(got, tt.want)
			assert.NoError(t, err)
			assert.True(t, equal)
		})
	}
}

func windowIntervalEqual(got, want []*window.IntervalWindow) (bool, error) {
	if len(got) != len(want) {
		return false, fmt.Errorf("len(got)=%d NOT EQUAL TO len(want)=%d", len(got), len(want))
	}

	for idx, intervalWindow := range want {
		if !got[idx].Start.Equal(intervalWindow.Start) || !got[idx].End.Equal(intervalWindow.End) {
			return false, fmt.Errorf("got Start %s != Want %s $| end Start %s != Want %s", got[idx].Start, got[idx].End, intervalWindow.Start, intervalWindow.End)
		}
	}

	return true, nil
}

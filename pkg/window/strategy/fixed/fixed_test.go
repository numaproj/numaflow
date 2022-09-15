package fixed

import (
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

func TestFixed_AssignWindow(t *testing.T) {

	loc, _ := time.LoadLocation("UTC")
	baseTime := time.Unix(1651129201, 0).In(loc)

	tests := []struct {
		name      string
		length    time.Duration
		eventTime time.Time
		want      []*window.IntervalWindow
	}{
		{
			name:      "minute",
			length:    time.Minute,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129260, 0).In(loc),
				},
			},
		},
		{
			name:      "hour",
			length:    time.Hour,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+3600, 0).In(loc),
				},
			},
		},
		{
			name:      "5_minute",
			length:    time.Minute * 5,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129200+300, 0).In(loc),
				},
			},
		},
		{
			name:      "30_second",
			length:    time.Second * 30,
			eventTime: baseTime,
			want: []*window.IntervalWindow{
				{
					Start: time.Unix(1651129200, 0).In(loc),
					End:   time.Unix(1651129230, 0).In(loc),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFixed(tt.length)
			if got := f.AssignWindow(tt.eventTime); !(got[0].Start.Equal(tt.want[0].Start) && got[0].End.Equal(tt.want[0].End)) {
				t.Errorf("AssignWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}

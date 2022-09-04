// Package sliding implements Sliding windows.  Sliding windows are defined by a window size and slide
// period, e.g. hourly windows starting every minute. Fixed windows are really a special case of sliding windows where
// size equals period.
package sliding

import (
	"fmt"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

// Sliding implements Sliding window.
type Sliding struct {
	Length          time.Duration
	PeriodInSeconds int
	sliceCount      int
}

var _ window.Windower = (*Sliding)(nil)

// NewSliding returns a Sliding window.
func NewSliding(length time.Duration, periodInSeconds int) *Sliding {
	sliceCount := int(length.Seconds() / float64(periodInSeconds))

	return &Sliding{
		Length:          length,
		PeriodInSeconds: periodInSeconds,
		sliceCount:      sliceCount,
	}
}

// AssignWindow assigns a window for the given eventTime.
func (s *Sliding) AssignWindow(eventTime time.Time) []*window.IntervalWindow {
	slidingWindows := make([]*window.IntervalWindow, s.sliceCount)

	tmp := eventTime.Truncate(s.Length)
	for i := 0; i < s.sliceCount; i++ {
		start := tmp
		slidingWindows[i] = &window.IntervalWindow{
			Start: start,
			End:   start.Add(time.Second * time.Duration(s.PeriodInSeconds)),
		}
		fmt.Println(slidingWindows[i].Start, slidingWindows[i].End)
		tmp = tmp.Add(-1 * time.Second * time.Duration(s.PeriodInSeconds))
	}
	return slidingWindows
}

// TrackWindowByKey tracks Sliding window.
func (s *Sliding) TrackWindowByKey(_ string, _ *window.IntervalWindow) {
	return
}

// Package sliding implements Sliding windows.  Sliding windows are defined by a window size and slide
// period, e.g. hourly windows starting every minute. Fixed windows are really a special case of sliding windows where
// size equals period.
package sliding

import (
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

// Sliding implements Sliding window.
type Sliding struct {
	// Length is the total length of the sliding window
	Length time.Duration
	// PeriodInSeconds is the time in seconds to slide in every step
	PeriodInSeconds int
	// sliceCount is the total number of slices in the window (Length/Period)
	sliceCount int
}

var _ window.Windower = (*Sliding)(nil)

// NewSliding returns a Sliding window.
func NewSliding(length time.Duration, periodInSeconds int) *Sliding {
	sliceCount := int(length.Seconds() / float64(periodInSeconds))

	// if the length is not exactly divisible, sliceCount needs to be bumped by 1
	if (length.Seconds() > float64(periodInSeconds)) && math.Mod(length.Seconds(), float64(periodInSeconds)) > 0 {
		sliceCount += 1
	}

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
			End:   start.Add(s.Length),
		}
		tmp = tmp.Add(-1 * time.Second * time.Duration(s.PeriodInSeconds))
	}
	return slidingWindows
}

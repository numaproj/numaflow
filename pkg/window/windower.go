package window

import (
	"time"
)

// Windower assigns the element to zero or more windows.
type Windower interface {
	// AssignWindow assigns the event to the window based on give window configuration.
	AssignWindow(eventTime time.Time) []*IntervalWindow
}

// IntervalWindow has the window boundary details.
type IntervalWindow struct {
	// Start is start time of the boundary which is inclusive.
	Start time.Time
	// End is the end time of the boundary and is exclusive.
	End time.Time
}

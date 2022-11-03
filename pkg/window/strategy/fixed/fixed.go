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

// Package fixed implements Fixed windows. Fixed windows (sometimes called tumbling windows) are
// defined by a static window size, e.g. minutely windows or hourly windows. They are generally aligned, i.e. every
// window applies across all the data for the corresponding period of time.
// Package fixed also maintains the state of active keyed windows in a vertex.
// Keyed Window maintains the association between set of keys and an interval window.
// keyed also provides the lifecycle management of an interval window. Watermark is used to trigger the expiration of windows.
package fixed

import (
	"time"

	"github.com/numaproj/numaflow/pkg/window"
)

// Fixed implements Fixed window.
type Fixed struct {
	// Length is the temporal length of the window.
	Length time.Duration
}

var _ window.Windower = (*Fixed)(nil)

// NewFixed returns a Fixed window.
func NewFixed(length time.Duration) *Fixed {
	return &Fixed{
		Length: length,
	}
}

// AssignWindow assigns a window for the given eventTime.
func (f *Fixed) AssignWindow(eventTime time.Time) []*window.IntervalWindow {
	start := eventTime.Truncate(f.Length)

	return []*window.IntervalWindow{
		{
			Start: start,
			End:   start.Add(f.Length),
		},
	}
}

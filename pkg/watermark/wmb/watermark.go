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

package wmb

import "time"

// Watermark is the monotonically increasing watermark. It is tightly coupled with ProcessorEntitier as
// the processor is responsible for monotonically increasing Watermark for that processor.
// NOTE: today we support only second progression of watermark, we need to support millisecond too.
type Watermark time.Time

var InitialWatermark = Watermark(time.UnixMilli(-1))

func (w Watermark) String() string {
	var location, _ = time.LoadLocation("UTC")
	var t = time.Time(w).In(location)
	return t.Format(time.RFC3339Nano)
}

func (w Watermark) UnixMilli() int64 {
	return time.Time(w).UnixMilli()
}

func (w Watermark) After(t time.Time) bool {
	return time.Time(w).After(t)
}

func (w Watermark) AfterWatermark(compare Watermark) bool {
	return w.After(time.Time(compare))
}

func (w Watermark) Before(t time.Time) bool {
	return time.Time(w).Before(t)
}

func (w Watermark) BeforeWatermark(compare Watermark) bool {
	return w.Before(time.Time(compare))
}

func (w Watermark) Add(t time.Duration) time.Time {
	return time.Time(w).Add(t)
}

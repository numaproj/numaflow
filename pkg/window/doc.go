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

// Package window implements windowing constructs. In the world of data processing on an unbounded stream, Windowing
// is a concept of grouping data using temporal boundaries. We use event-time to discover temporal boundaries on an
// unbounded, infinite stream and Watermark to ensure the datasets within the boundaries are complete. A reduce function
// can be applied on this group of data.
//
// Windows are of different types, quite popular ones are Fixed windows and Sliding windows. Sessions are managed via
// little less popular windowing strategy called Session windows. Windowing is implemented as a two stage process,
//   - Assign windows - assign the event to a window
//   - Merge windows - group all the events that below to the same window
//
// The two stage approach is required because assignment of windows could happen as elements are streaming in, but merging
// could happen before the data materialization happens. This is important esp. when we handle session windows where a
// new event can change the end time of the window.
//
// For simplicity, we will be truncating the windows' boundaries to the nearest time unit (say, 1 minute windows will
// be truncated to 0th second). Truncating window time to the nearest boundary will help us do mapping with constant time
// without affecting the correctness, except for the very first materialization of result (e.g., we started at 9:00.11
// and the result will be materialized at 9:01.00 and not at 9:01:11).
//
// Windows may be either aligned (e.g., Fixed, Sliding), i.e. applied across all the data for the window of time in
// question, or unaligned, (e.g., Session) i.e. applied across only specific subsets of the data (e.g. per key) for the
// given window of time.
package window

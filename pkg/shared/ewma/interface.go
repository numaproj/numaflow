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

package ewma

// EWMA is the interface for Exponentially Weighted Moving Average
// It is used to calculate the moving average with decay of a series of numbers
type EWMA interface {
	// Add adds a new value to the EWMA
	Add(float64)
	// Get returns the current value of the EWMA
	Get() float64
	// Reset resets the EWMA to the initial value
	Reset()
	// Set sets the EWMA to the given value
	Set(float64)
}

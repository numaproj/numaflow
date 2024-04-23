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

const (
	defaultAlpha = 30.0
	// constDecayFactor is the default decay factor
	constDecayFactor = 2.0 / (defaultAlpha + 1.0)
)

// SimpleEWMA is a simple implementation of EWMA
type SimpleEWMA struct {
	// alpha is the smoothing factor
	alpha float64
	// value is the current value of the EWMA
	value float64
	// init is a flag to indicate if the EWMA has been initialized
	init bool
}

// NewSimpleEWMA returns a new SimpleEWMA
// If the alpha is not provided we use a default value of constDecayFactor
// If the alpha is provided we calulate the smoothing factor from it
func NewSimpleEWMA(alpha ...float64) *SimpleEWMA {
	if len(alpha) > 0 {
		decay := 2.0 / (alpha[0] + 1.0)
		return &SimpleEWMA{alpha: decay}
	}
	return &SimpleEWMA{alpha: constDecayFactor}
}

// Add adds a new value to the EWMA
func (s *SimpleEWMA) Add(value float64) {
	// If the EWMA has not been initialized, set the value and return
	if !s.init {
		s.value = value
		s.init = true
		return
	}
	// Otherwise, calculate the EWMA
	s.value = s.value + s.alpha*(value-s.value)
}

// Get returns the current value of the EWMA
func (s *SimpleEWMA) Get() float64 {
	return s.value
}

// Reset resets the EWMA to the initial value
func (s *SimpleEWMA) Reset() {
	s.value = 0
	s.init = false
}

// Set sets the EWMA to the given value
func (s *SimpleEWMA) Set(value float64) {
	s.value = value
}

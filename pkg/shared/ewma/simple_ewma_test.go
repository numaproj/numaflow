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

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	defaultEWMA = 77.14075212282631
	span15EWMA  = 74.9112723022807
)

var samples = [14]float64{
	83.92333333333333, 0, 83.24000000000001, 88.24, 77.61, 76.57333333333334, 79.91333333333334, 80.34,
	74.90666666666667, 69.90666666666667, 71.65, 73.19333333333333, 72.18666666666667, 74.90666666666667,
}

// TestSimpleEWMA tests the SimpleEWMA implementation.
func TestSimpleEWMA(t *testing.T) {
	// Create a new EWMA with the default decay factor.
	newEwma := NewSimpleEWMA()
	for _, f := range samples {
		newEwma.Add(f)
	}
	// Check if the value is within the margin of error.
	assert.True(t, math.Abs(defaultEWMA-newEwma.Get()) < 0.00000001)

	// Create a new EWMA with a custom decay factor.
	newEwma = NewSimpleEWMA(15)
	for _, f := range samples {
		newEwma.Add(f)
	}
	// Check if the value is within the margin of error.
	assert.True(t, math.Abs(span15EWMA-newEwma.Get()) < 0.00000001)
}

// TestSimpleEWMAInit tests the SimpleEWMA initialization.

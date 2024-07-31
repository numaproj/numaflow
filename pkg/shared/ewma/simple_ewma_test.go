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
func TestSimpleEWMAAdd(t *testing.T) {
	ewma := NewSimpleEWMA()

	// Test initialization
	ewma.Add(10)
	assert.Equal(t, 10.0, ewma.Get())
}

func TestSimpleEWMAGet(t *testing.T) {
	ewma := NewSimpleEWMA()

	// Test get before initialization
	assert.Equal(t, 0.0, ewma.Get())
}

func TestSimpleEWMAReset(t *testing.T) {
	ewma := NewSimpleEWMA()

	ewma.Add(100)
	assert.Equal(t, 100.0, ewma.Get())

	ewma.Reset()
	assert.Equal(t, 0.0, ewma.Get())

	// Test that the next Add after Reset initializes properly
	ewma.Add(200)
	assert.Equal(t, 200.0, ewma.Get())
}

func TestSimpleEWMASet(t *testing.T) {
	ewma := NewSimpleEWMA()

	ewma.Set(75)
	assert.Equal(t, 75.0, ewma.Get())
}

func TestSimpleEWMAWithCustomAlpha(t *testing.T) {
	ewma := NewSimpleEWMA(10) // span of 10 gives alpha of 0.18181818...

	ewma.Add(50)
	ewma.Add(100)
	assert.InDelta(t, 59.09090909, ewma.Get(), 0.00000001)
}

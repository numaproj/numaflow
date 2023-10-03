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

// WMBChecker checks if the idle watermark is valid. It checks by making sure the Idle WMB's offset has not been
// changed in X iterations. This check is required because we have to make sure the time at which the idleness has been
// detected matches with reality (processes could hang in between). The only way to do that is by multiple iterations.
type WMBChecker struct {
	iterationCounter int
	iterations       int
	w                WMB
}

// NewWMBChecker returns a WMBChecker to check if the wmb is idle.
// If all the iterations get the same wmb offset, the wmb is considered as valid
// and will be used to publish a wmb to the toBuffer partitions of the next vertex.
func NewWMBChecker(numOfIteration int) WMBChecker {
	return WMBChecker{
		iterationCounter: 0,
		iterations:       numOfIteration,
		w:                WMB{},
	}
}

// ValidateHeadWMB checks if the head wmb is idle, and it has the same wmb offset from the previous iteration.
// If all the iterations get the same wmb offset, returns true.
func (c *WMBChecker) ValidateHeadWMB(w WMB) bool {
	if !w.Idle {
		// if wmb is not idle, skip and reset the iterationCounter
		c.iterationCounter = 0
		return false
	}
	// check the iterationCounter value
	if c.iterationCounter == 0 {
		c.iterationCounter++
		// the wmb only writes once when iterationCounter is zero
		c.w.Offset = w.Offset
	} else if c.iterationCounter < c.iterations-1 {
		c.iterationCounter++
		if c.w.Offset == w.Offset {
			// we get the same wmb, meaning the wmb is valid, continue
		} else {
			// else, start over
			c.iterationCounter = 0
		}
	} else if c.iterationCounter >= c.iterations-1 {
		c.iterationCounter = 0
		if c.w.Offset == w.Offset {
			// reach max iteration, if still get the same wmb,
			// then the wmb is considered as valid, return ture
			return true
		}
	}
	return false
}

// GetCounter gets the current iterationCounter value for the WMBChecker, it's used in log and tests
func (c *WMBChecker) GetCounter() int {
	return c.iterationCounter
}

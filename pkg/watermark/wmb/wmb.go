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

// Package wmb represents the offset-timeline pair and its corresponding encoder and decoder.
package wmb

import (
	"bytes"
	"encoding/binary"
)

// WMB is used in the KV offset timeline bucket as the value for the given processor entity key.
type WMB struct {
	// Idle is set to true if the given processor entity hasn't published anything
	// to the offset timeline bucket in a batch processing cycle.
	// Idle is used to signal an idle watermark.
	Idle bool
	// Offset is the monotonically increasing index/offset of the buffer (buffer is the physical representation
	// of the partition of the edge).
	Offset int64
	// Watermark is tightly coupled with the offset and will be monotonically increasing for a given ProcessorEntity
	// as the offset increases.
	// When it is idling (Idle==true), for a given offset, the watermark can monotonically increase without offset
	// increasing.
	Watermark int64
}

// EncodeToBytes encodes a WMB object into byte array.
func (w WMB) EncodeToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, w)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeToWMB decodes the given byte array into a WMB object.
func DecodeToWMB(b []byte) (WMB, error) {
	var v WMB
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &v)
	if err != nil {
		return WMB{}, err
	}
	return v, nil
}

type WMBChecker struct {
	counter int
	max     int
	w       WMB
}

// NewWMBChecker returns a WMBChecker to check if the wmb is idle.
// If all the iterations get the same wmb, the wmb is considered as valid
// and will be used to publish a wmb to pods of the next vertex.
func NewWMBChecker(numOfIteration int) WMBChecker {
	return WMBChecker{
		counter: 0,
		max:     numOfIteration,
		w:       WMB{},
	}
}

// ValidateHeadWMB checks if the head wmb is idle and is the same as the wmb from the previous iteration.
// If all the iterations get the same wmb, returns true.
func (c *WMBChecker) ValidateHeadWMB(w WMB) bool {
	if !w.Idle {
		// if wmb is not idle, skip and reset the counter
		c.counter = 0
		return false
	}
	// check the counter value
	if c.counter == 0 {
		c.counter++
		// the wmb only writes once when counter is zero
		c.w = w
	} else if c.counter < c.max-1 {
		c.counter++
		if c.w == w {
			// we get the same wmb, meaning the wmb is valid, continue
		} else {
			// else, start over
			c.counter = 0
		}
	} else if c.counter >= c.max-1 {
		c.counter = 0
		if c.w == w {
			// reach max iteration, if still get the same wmb,
			// then the wmb is considered as valid, return ture
			return true
		}
	}
	return false
}

// GetCounter gets the current counter value for the WMBChecker, it's used in log and tests
func (c *WMBChecker) GetCounter() int {
	return c.counter
}

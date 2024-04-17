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

package timeline

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// OffsetTimeline is to store the event time to the offset records.
// Our list is sorted by event time from highest to lowest.
type OffsetTimeline struct {
	ctx context.Context
	// TODO: replace it with OverflowQueue, which is thread safe and 2 times faster.
	watermarks list.List
	capacity   int
	lock       sync.RWMutex
	log        *zap.SugaredLogger
}

// NewOffsetTimeline returns OffsetTimeline.
func NewOffsetTimeline(ctx context.Context, c int) *OffsetTimeline {
	// Initialize a new empty watermarks DLL with nil values of the size capacity.
	// This is to avoid length check: when a new element is added, the tail element will be deleted.
	offsetTimeline := OffsetTimeline{
		ctx:      ctx,
		capacity: c,
		log:      logging.FromContext(ctx),
	}

	for i := 0; i < c; i++ {
		offsetTimeline.watermarks.PushBack(wmb.WMB{
			Watermark: -1,
			Offset:    -1,
		})
	}

	return &offsetTimeline
}

// Capacity returns the capacity of the OffsetTimeline list.
func (t *OffsetTimeline) Capacity() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.capacity
}

// Put inserts the WMB into list. It ensures that the list will remain sorted after the insert.
func (t *OffsetTimeline) Put(node wmb.WMB) {
	t.lock.Lock()
	defer t.lock.Unlock()
	// The `for` loop's amortized time complexity should be O(1). Since the OffsetTimeline is sorted by time and
	// our access pattern will always hit the most recent data.
	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		var elementNode = e.Value.(wmb.WMB)
		if node.Watermark == elementNode.Watermark {
			// we store the maximum Offset for the given event time
			if node.Offset > elementNode.Offset {
				e.Value = wmb.WMB{
					Watermark: node.Watermark,
					Offset:    node.Offset,
				}
				return
			} else {
				// This can happen if the publisher of the watermark is a Map Vertex reading from >1 partition. In that case, there is one goroutine per
				// partition and the following example can happen some of the time for two goroutines G1 and G2 both publishing to the same OffsetTimeline:
				// 1. G1 determines watermark x and publishes to offsets 100-101
				// 2. G2 determines watermark x and publishes to offsets 102-103
				// 3. G2 publishes watermark x for offset 103
				// 4. G1 publishes watermark x for offset 101
				t.log.Debugw("Watermark the same but Input offset smaller than the existing offset - skipping", zap.Int64("watermark", node.Watermark),
					zap.Int64("existingOffset", elementNode.Offset), zap.Int64("inputOffset", node.Offset))
				return
			}
		} else if node.Watermark > elementNode.Watermark {
			if node.Offset < elementNode.Offset {
				t.log.Errorw("The new input offset should never be smaller than the existing offset", zap.Int64("watermark", node.Watermark), zap.Int64("existingWatermark", elementNode.Watermark),
					zap.Int64("existingOffset", elementNode.Offset), zap.Int64("inputOffset", node.Offset))
				return
			}
			// our list is sorted by event time from highest to lowest
			t.watermarks.InsertBefore(node, e)
			// remove the last event time
			t.watermarks.Remove(t.watermarks.Back())
			return
		} else {
			// keep iterating, we need to go to the next smallest watermark.
			continue
		}
	}
}

// PutIdle inserts the idle WMB into list or update the current idle WMB.
func (t *OffsetTimeline) PutIdle(node wmb.WMB) {
	t.lock.Lock()
	defer t.lock.Unlock()
	// when inserting an idle WMB, we only need to compare with the head
	// and can safely skip insertion when any condition doesn't meet
	if e := t.watermarks.Front(); e != nil {
		var elementNode = e.Value.(wmb.WMB)
		if elementNode.Idle {
			if node.Watermark < elementNode.Watermark {
				// should never happen
				return
			}
			if elementNode.Offset == node.Offset {
				e.Value = wmb.WMB{
					Idle:      true,
					Offset:    node.Offset,
					Watermark: node.Watermark,
				}
			} else if node.Offset > elementNode.Offset {
				// NOTE: Valid case for the warning:
				// If we have an active watermark between two idle watermarks, but this active watermark is skipped
				// publishing due to "skip publishing same watermark", we will see this warning.
				// In this case, it's safe to insert the idle watermark.
				t.log.Warnw("The idle watermark has a larger offset from the head idle watermark", zap.Int64("idleWatermark", node.Watermark),
					zap.Int64("existingOffset", elementNode.Offset), zap.Int64("inputOffset", node.Offset))
				t.watermarks.InsertBefore(node, e)
				t.watermarks.Remove(t.watermarks.Back())
			}
			return
		}
		if node.Watermark > elementNode.Watermark {
			if node.Offset > elementNode.Offset {
				t.watermarks.InsertBefore(node, e)
				t.watermarks.Remove(t.watermarks.Back())
				return
			}
		} else if node.Watermark == elementNode.Watermark {
			if node.Offset > elementNode.Offset {
				e.Value = wmb.WMB{
					Idle:      true,
					Watermark: node.Watermark,
					Offset:    node.Offset,
				}
				return
			}
		} else {
			return
		}
	}
}

// GetHeadOffset returns the head offset, that is the most recent offset which will have the highest
// Watermark.
func (t *OffsetTimeline) GetHeadOffset() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.watermarks.Len() == 0 {
		return -1
	}
	return t.watermarks.Front().Value.(wmb.WMB).Offset
}

// GetHeadWatermark returns the head watermark, which is the highest one.
func (t *OffsetTimeline) GetHeadWatermark() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.watermarks.Len() == 0 {
		return 0
	}
	return t.watermarks.Front().Value.(wmb.WMB).Watermark
}

// GetHeadWMB returns the largest offset with the largest watermark.
func (t *OffsetTimeline) GetHeadWMB() wmb.WMB {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.watermarks.Front().Value.(wmb.WMB)
}

// GetOffset will return the offset for the given event-time.
// TODO(jyu6): will make Watermark an interface make it easy to pass an Offset and return a Watermark?
func (t *OffsetTimeline) GetOffset(eventTime int64) int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()

	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		if eventTime >= e.Value.(wmb.WMB).Watermark {
			return e.Value.(wmb.WMB).Offset
		}
	}

	// reach the end of the list, not found
	return -1
}

// GetEventTime will return the event-time for the given offset.
// TODO(jyu6): will make Watermark an interface make it easy to get a Watermark and return an Offset?
func (t *OffsetTimeline) GetEventTime(inputOffset isb.Offset) int64 {
	// TODO: handle err?
	inputOffsetInt64, _ := inputOffset.Sequence()
	return t.GetEventTimeFromInt64(inputOffsetInt64)
}

func (t *OffsetTimeline) GetEventTimeFromInt64(inputOffsetInt64 int64) int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()

	var eventTime int64 = -1

	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		// get the event time has the closest offset to the input offset
		// exclude the same offset because this offset may not finish processing yet
		if e.Value.(wmb.WMB).Offset < inputOffsetInt64 {
			eventTime = e.Value.(wmb.WMB).Watermark
			break
		}
	}

	return eventTime
}

// Dump dumps the in-memory representation of the OffsetTimeline. Could get very ugly if the list is large, like > 100 elements.
// I am assuming we will have it in 10K+ (86400 seconds are there in a day).
func (t *OffsetTimeline) Dump() string {
	var builder strings.Builder
	t.lock.RLock()
	defer t.lock.RUnlock()
	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		if e.Value.(wmb.WMB).Idle {
			builder.WriteString(fmt.Sprintf("[IDLE %d:%d] -> ", e.Value.(wmb.WMB).Watermark, e.Value.(wmb.WMB).Offset))
		} else {
			builder.WriteString(fmt.Sprintf("[%d:%d] -> ", e.Value.(wmb.WMB).Watermark, e.Value.(wmb.WMB).Offset))
		}
	}
	if builder.Len() < 4 {
		return ""
	}
	return builder.String()[:builder.Len()-4]
}

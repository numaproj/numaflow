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

package fetch

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

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
		offsetTimeline.watermarks.PushBack(OffsetWatermark{
			watermark: -1,
			offset:    -1,
		})
	}

	return &offsetTimeline
}

// OffsetWatermark stores the maximum offset for the given event time
// we use basic data type int64 to compare the value
type OffsetWatermark struct {
	// watermark is derived from fetch.Watermark
	watermark int64
	// offset is derived from isb.Offset
	offset int64
}

// Capacity returns the capacity of the OffsetTimeline list.
func (t *OffsetTimeline) Capacity() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.capacity
}

// Put inserts the OffsetWatermark into list. It ensures that the list will remain sorted after the insert.
func (t *OffsetTimeline) Put(node OffsetWatermark) {
	t.lock.Lock()
	defer t.lock.Unlock()
	// The `for` loop's amortized time complexity should be O(1). Since the OffsetTimeline is sorted by time and
	// our access pattern will always hit the most recent data.
	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		var elementNode = e.Value.(OffsetWatermark)
		if node.watermark == elementNode.watermark {
			// we store the maximum offset for the given event time
			if node.offset > elementNode.offset {
				e.Value = OffsetWatermark{
					watermark: node.watermark,
					offset:    node.offset,
				}
				return
			} else {
				// TODO put panic: the new input offset should never be smaller than the existing offset
				t.log.Errorw("The new input offset should never be smaller than the existing offset", zap.Int64("watermark", node.watermark),
					zap.Int64("existing offset", elementNode.offset), zap.Int64("input offset", node.offset))
				return
			}
		} else if node.watermark > elementNode.watermark {
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

// GetHeadOffset returns the head offset, that is the most recent offset which will have the highest
// Watermark.
func (t *OffsetTimeline) GetHeadOffset() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.watermarks.Len() == 0 {
		return -1
	}
	return t.watermarks.Front().Value.(OffsetWatermark).offset
}

// GetHeadWatermark returns the head watermark, which is the highest one.
func (t *OffsetTimeline) GetHeadWatermark() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.watermarks.Len() == 0 {
		return 0
	}
	return t.watermarks.Front().Value.(OffsetWatermark).watermark
}

// GetTailOffset returns the smallest offset with the smallest watermark.
func (t *OffsetTimeline) GetTailOffset() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.watermarks.Len() == 0 {
		return -1
	}
	return t.watermarks.Back().Value.(OffsetWatermark).offset
}

// GetOffset will return the offset for the given event-time.
// TODO(jyu6): will make Watermark an interface make it easy to pass an Offset and return a Watermark?
func (t *OffsetTimeline) GetOffset(eventTime int64) int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()

	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		if eventTime >= e.Value.(OffsetWatermark).watermark {
			return e.Value.(OffsetWatermark).offset
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

	var (
		offset    int64 = -1
		eventTime int64 = -1
	)

	for e := t.watermarks.Front(); e != nil; e = e.Next() {
		// get the event time has the closest offset to the input offset
		// exclude the same offset because this offset may not finish processing yet
		// offset < e.Value.(OffsetWatermark).offset: use < because we want the largest possible timestamp
		if offset < e.Value.(OffsetWatermark).offset && e.Value.(OffsetWatermark).offset < inputOffsetInt64 {
			offset = e.Value.(OffsetWatermark).offset
			eventTime = e.Value.(OffsetWatermark).watermark
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
		builder.WriteString(fmt.Sprintf("[%d:%d] -> ", e.Value.(OffsetWatermark).watermark, e.Value.(OffsetWatermark).offset))
	}
	if builder.Len() < 4 {
		return ""
	}
	return builder.String()[:builder.Len()-4]
}

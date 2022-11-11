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

/*
Package processor is the smallest processor entity for which the watermark will strictly monotonically increase.
*/
package processor

import (
	"time"
)

// Watermark is the monotonically increasing watermark. It is tightly coupled with ProcessorEntitier as
// the processor is responsible for monotonically increasing Watermark for that processor.
// NOTE: today we support only second progression of watermark, we need to support millisecond too.
type Watermark time.Time

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

func (w Watermark) Before(t time.Time) bool {
	return time.Time(w).Before(t)
}

// ProcessorEntitier defines what can be a processor. The Processor is the smallest unit where the watermark will
// monotonically increase.
type ProcessorEntitier interface {
	GetID() string
	BuildOTWatcherKey() string
}

// processorEntity implements ProcessorEntitier.
type processorEntity struct {
	// name is the name of the entity
	name string
}

var _ ProcessorEntitier = (*processorEntity)(nil)

// NewProcessorEntity returns a new `ProcessorEntitier`.
func NewProcessorEntity(name string) ProcessorEntitier {
	return &processorEntity{
		name: name,
	}
}

// GetID returns the ID of the processor.
func (p *processorEntity) GetID() string {
	return p.name
}

// BuildOTWatcherKey builds the offset-timeline key name
func (p *processorEntity) BuildOTWatcherKey() string {
	return p.GetID()
}

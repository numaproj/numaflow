/*
Package processor is the smallest processor entity for which the watermark will strictly monotonically increase.
*/
package processor

import (
	"time"
)

// Watermark is the monotonically increasing watermark. It is tightly coupled with ProcessorEntity as
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

// ProcessorEntity implements ProcessorEntitier.
type ProcessorEntity struct {
	// name is the name of the entity
	name string
}

var _ ProcessorEntitier = (*ProcessorEntity)(nil)

// NewProcessorEntity returns a new `ProcessorEntity`.
func NewProcessorEntity(name string) *ProcessorEntity {
	return &ProcessorEntity{
		name: name,
	}
}

// GetID returns the ID of the processor.
func (p *ProcessorEntity) GetID() string {
	return p.name
}

// BuildOTWatcherKey builds the offset-timeline key name
func (p *ProcessorEntity) BuildOTWatcherKey() string {
	return p.GetID()
}

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
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	entity2 "github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/timeline"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type status int

const (
	_active status = iota
	_inactive
	_deleted
)

func (s status) String() string {
	switch s {
	case _active:
		return "active"
	case _inactive:
		return "inactive"
	case _deleted:
		return "deleted"
	}
	return "unknown"
}

// ProcessorToFetch is the smallest unit of entity (from which we fetch data) that does inorder processing or contains inorder data. It tracks OT for all the partitions of the from buffer.
type ProcessorToFetch struct {
	ctx    context.Context
	entity entity2.ProcessorEntitier
	status status
	// offsetTimelines is a slice of OTs for each partition of the incoming buffer.
	offsetTimelines []*timeline.OffsetTimeline
	lock            sync.RWMutex
	log             *zap.SugaredLogger
}

// GetEntity returns the processor entity.
func (p *ProcessorToFetch) GetEntity() entity2.ProcessorEntitier {
	return p.entity
}

// GetOffsetTimelines returns the processor's OT.
func (p *ProcessorToFetch) GetOffsetTimelines() []*timeline.OffsetTimeline {
	return p.offsetTimelines
}

func (p *ProcessorToFetch) String() string {
	var stringBuilder strings.Builder
	for _, ot := range p.offsetTimelines {
		stringBuilder.WriteString(fmt.Sprintf(" %s\n ", ot.Dump()))
	}
	return fmt.Sprintf("%s status:%v, timelines: %s", p.entity.GetName(), p.getStatus(), stringBuilder.String())
}

// NewProcessorToFetch creates ProcessorToFetch.
func NewProcessorToFetch(ctx context.Context, processor entity2.ProcessorEntitier, capacity int, fromBufferPartitionCount int32) *ProcessorToFetch {

	var offsetTimelines []*timeline.OffsetTimeline
	for i := int32(0); i < fromBufferPartitionCount; i++ {
		t := timeline.NewOffsetTimeline(ctx, capacity)
		offsetTimelines = append(offsetTimelines, t)
	}
	p := &ProcessorToFetch{
		ctx:             ctx,
		entity:          processor,
		status:          _active,
		offsetTimelines: offsetTimelines,
		log:             logging.FromContext(ctx),
	}
	return p
}

func (p *ProcessorToFetch) setStatus(s status) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.status = s
}

func (p *ProcessorToFetch) getStatus() status {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status
}

// IsActive returns whether a processor is active.
func (p *ProcessorToFetch) IsActive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _active
}

// IsInactive returns whether a processor is inactive (no heartbeats or any sort).
func (p *ProcessorToFetch) IsInactive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _inactive
}

// IsDeleted returns whether a processor has been deleted.
func (p *ProcessorToFetch) IsDeleted() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _deleted
}

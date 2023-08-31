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
package entity

// ProcessorEntitier defines what can be a processor. The Processor is the smallest unit where the watermark will
// monotonically increase.
type ProcessorEntitier interface {
	GetName() string
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

// GetName returns the ID of the processor.
func (p *processorEntity) GetName() string {
	return p.name
}

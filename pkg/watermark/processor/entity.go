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
	"fmt"
	"strconv"
	"strings"
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

type entityOptions struct {
	keySeparator string
}

// EntityOption set options for FromVertex.
type EntityOption func(*entityOptions)

// ProcessorEntitier defines what can be a processor. The Processor is the smallest unit where the watermark will
// monotonically increase.
type ProcessorEntitier interface {
	GetID() string
	BuildOTWatcherKey(Watermark) string
	ParseOTWatcherKey(string) (int64, bool, error)
}

// ProcessorEntity implements ProcessorEntitier.
type ProcessorEntity struct {
	// name is the name of the entity
	name string
	opts *entityOptions
}

var _ ProcessorEntitier = (*ProcessorEntity)(nil)

// _defaultKeySeparator is the key separate used in the offset timeline kv.
// NOTE: we can only use `_` as the separator, JetStream will not let any other special character.
//
//	Perhaps we can encode the key using base64, but it will have a performance hit.
const _defaultKeySeparator = "_"

// NewProcessorEntity returns a new `ProcessorEntity`.
func NewProcessorEntity(name string, inputOpts ...EntityOption) *ProcessorEntity {
	opts := &entityOptions{
		keySeparator: _defaultKeySeparator,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}
	return &ProcessorEntity{
		name: name,
		opts: opts,
	}
}

// GetID returns the ID of the processor.
func (p *ProcessorEntity) GetID() string {
	return p.name
}

// BuildOTWatcherKey builds the offset-timeline key name
func (p *ProcessorEntity) BuildOTWatcherKey(watermark Watermark) string {
	return fmt.Sprintf("%s%s%d", p.GetID(), p.opts.keySeparator, watermark.UnixMilli())
}

// ParseOTWatcherKey parses the key of the KeyValue OT watcher and returns the epoch, a boolean to indicate
// whether the record can be skipped and error if any.
// NOTE: _defaultKeySeparator has constraints, please make sure we will not end up with multiple values
func (p *ProcessorEntity) ParseOTWatcherKey(key string) (epoch int64, skip bool, err error) {
	name, epochStr, err := p.splitKey(key)
	if err != nil {
		return 0, false, err
	}
	// skip if not this processor
	skip = name != p.GetID()
	epoch, err = strconv.ParseInt(epochStr, 10, 64)

	return epoch, skip, err
}

func (p *ProcessorEntity) splitKey(key string) (string, string, error) {
	split := strings.Split(key, p.opts.keySeparator)
	if len(split) != 2 {
		return "", "", fmt.Errorf("key=%s when split using %s, did not have 2 outputs=%v", key, p.opts.keySeparator, split)
	}
	return split[0], split[1], nil
}

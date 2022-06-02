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
	return t.Format(time.RFC3339)

}

type entityOptions struct {
	separateOTBucket bool
	keySeparator     string
}

// EntityOption set options for FromVertex.
type EntityOption func(*entityOptions)

// WithSeparateOTBuckets creates a different bucket for maintaining each processor offset-timeline.
func WithSeparateOTBuckets(separate bool) EntityOption {
	return func(opts *entityOptions) {
		opts.separateOTBucket = separate
	}
}

// ProcessorEntitier defines what can be a processor. The Processor is the smallest unit where the watermark will
// monotonically increase.
type ProcessorEntitier interface {
	GetID() string
	GetBucketName() string
	BuildOTWatcherKey(Watermark) string
	ParseOTWatcherKey(string) (int64, bool, error)
	IsSharedBucket() bool
	GetPublishKeyspace() string
}

// ProcessorEntity implements ProcessorEntitier.
type ProcessorEntity struct {
	// name is the name of the entity
	name            string
	publishKeyspace string
	opts            *entityOptions
}

// _defaultKeySeparator is the key separate when we have shared OT buckets.
// NOTE: we can only use `_` as the separator, Jetstream will not let any other special character.
//       Perhaps we can encode the key using base64, but it will have a performance hit.
const _defaultKeySeparator = "_"

// NewProcessorEntity returns a new `ProcessorEntity`
func NewProcessorEntity(name string, publishKeyspace string, inputOpts ...EntityOption) *ProcessorEntity {
	opts := &entityOptions{
		separateOTBucket: false,
		keySeparator:     _defaultKeySeparator,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}
	return &ProcessorEntity{
		name:            name,
		publishKeyspace: publishKeyspace,
		opts:            opts,
	}
}

// GetID returns the ID of the processor.
func (p *ProcessorEntity) GetID() string {
	return p.name
}

// GetPublishKeyspace returns the publishKeyspace of the entity
func (p *ProcessorEntity) GetPublishKeyspace() string {
	return p.publishKeyspace
}

// GetBucketName returns the offset-timeline for the entity.
func (p *ProcessorEntity) GetBucketName() string {
	if p.opts.separateOTBucket {
		return p.publishKeyspace + "_OT_" + p.name
	} else {
		return p.publishKeyspace + "_OT"
	}
}

// IsSharedBucket returns true if the bucket is shared.
func (p *ProcessorEntity) IsSharedBucket() bool {
	return p.opts.separateOTBucket
}

// BuildOTWatcherKey builds the offset-timeline key name
func (p *ProcessorEntity) BuildOTWatcherKey(watermark Watermark) string {
	if p.opts.separateOTBucket {
		return fmt.Sprintf("%d", time.Time(watermark).Unix())
	} else {
		return fmt.Sprintf("%s%s%d", p.GetID(), p.opts.keySeparator, time.Time(watermark).Unix())
	}
}

// ParseOTWatcherKey parses the key of the KeyValue OT watcher and returns the epoch, a boolean to indicate
// whether the record can be skipped and error if any.
// NOTE: _defaultKeySeparator has constraints, please make sure we will not end up with multiple values
func (p *ProcessorEntity) ParseOTWatcherKey(key string) (epoch int64, skip bool, err error) {
	var name string
	var epochStr = key
	// if not separate bucket, the key will have to be split
	if !p.opts.separateOTBucket {
		name, epochStr, err = p.splitKey(key)
		if err != nil {
			return 0, false, err
		}
		// skip if not this processor
		skip = name != p.GetID()
	}
	epoch, err = strconv.ParseInt(epochStr, 10, 64)

	return epoch, skip, err
}

func (p *ProcessorEntity) splitKey(key string) (string, string, error) {
	// if there are separate buckets, the name will be not present and the key is the epoch
	if p.opts.separateOTBucket {
		return "", key, nil
	}
	split := strings.Split(key, p.opts.keySeparator)
	if len(split) != 2 {
		return "", "", fmt.Errorf("key=%s when split using %s, did not have 2 outputs=%v", key, p.opts.keySeparator, split)
	}
	return split[0], split[1], nil
}

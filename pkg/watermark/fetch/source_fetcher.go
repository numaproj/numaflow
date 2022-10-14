package fetch

import (
	"context"
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"go.uber.org/zap"
)

// sourceFetcher is a fetcher on source buffers.
type sourceFetcher struct {
	ctx              context.Context
	sourceBufferName string
	processorManager *ProcessorManager
	log              *zap.SugaredLogger
}

// NewSourceFetcher returns a new source fetcher, processorManager has the details about the processors responsible for writing to the
// buckets of the source buffer.
func NewSourceFetcher(ctx context.Context, sourceBufferName string, processorManager *ProcessorManager) Fetcher {
	return &sourceFetcher{
		ctx:              ctx,
		sourceBufferName: sourceBufferName,
		processorManager: processorManager,
		log:              logging.FromContext(ctx).With("sourceBufferName", sourceBufferName),
	}
}

// GetHeadWatermark returns the latest watermark of all the processors.
func (e *sourceFetcher) GetHeadWatermark() processor.Watermark {
	e.log.Debugf("Fetching head watermark from source buffer %s...", e.sourceBufferName)
	var epoch int64 = math.MinInt64
	for _, p := range e.processorManager.GetAllProcessors() {
		if !p.IsActive() {
			continue
		}
		if p.offsetTimeline.GetHeadWatermark() > epoch {
			epoch = p.offsetTimeline.GetHeadWatermark()
		}
	}
	if epoch == math.MinInt64 {
		e.log.Debugf("Didn't find any existing head watermarks across all processors, using default watermark %d", int64(-1))
		return processor.Watermark(time.Unix(-1, 0))
	}
	e.log.Debugf("Found head watermark epoch %d", epoch)
	return processor.Watermark(time.Unix(epoch, 0))
}

// GetWatermark returns the lowest of the latest watermark of all the processors,
// it ignores the input offset.
func (e *sourceFetcher) GetWatermark(_ isb.Offset) processor.Watermark {
	var epoch int64 = math.MaxInt64
	for _, p := range e.processorManager.GetAllProcessors() {
		if !p.IsActive() {
			continue
		}
		if p.offsetTimeline.GetHeadWatermark() < epoch {
			epoch = p.offsetTimeline.GetHeadWatermark()
		}
	}
	if epoch == math.MaxInt64 {
		return processor.Watermark(time.Time{})
	}
	return processor.Watermark(time.Unix(epoch, 0))
}

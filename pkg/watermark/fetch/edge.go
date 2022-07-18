package fetch

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"go.uber.org/zap"
)

// Fetcher fetches watermark data from Vn-1 vertex.
type Fetcher interface {
	// GetWatermark returns the inorder monotonically increasing watermark of the edge connected to Vn-1.
	GetWatermark(offset isb.Offset) processor.Watermark
	// GetHeadWatermark returns the latest watermark based on the head offset
	GetHeadWatermark() processor.Watermark
}

// Edge is the edge relation between two vertices.
type Edge struct {
	ctx        context.Context
	edgeName   string
	fromVertex *FromVertex
	log        *zap.SugaredLogger
}

// NewEdgeBuffer returns a new Edge. FromVertex has the details about the processors responsible for writing to this
// edge.
func NewEdgeBuffer(ctx context.Context, edgeName string, fromV *FromVertex) *Edge {
	return &Edge{
		ctx:        ctx,
		edgeName:   edgeName,
		fromVertex: fromV,
		log:        logging.FromContext(ctx).With("edgeName", edgeName),
	}
}

// GetHeadWatermark returns the watermark using the HeadOffset (latest offset). This
// can be used in showing the watermark progression for a vertex when not consuming the messages
// directly (eg. UX, tests,)
func (e *Edge) GetHeadWatermark() processor.Watermark {
	var debugString strings.Builder
	var headOffset int64 = math.MinInt64
	var epoch int64 = math.MaxInt64
	var allProcessors = e.fromVertex.GetAllProcessors()
	// get the head offset of each processor
	for _, p := range allProcessors {
		debugString.WriteString(fmt.Sprintf("[HB:%s OT:%s] (headoffset:%d) %s\n", e.fromVertex.hbWatcher.GetKVName(), e.fromVertex.otWatcher.GetKVName(), p.offsetTimeline.GetHeadOffset(), p))
		var o = p.offsetTimeline.GetHeadOffset()
		if o != -1 && o > headOffset {
			headOffset = o
			epoch = p.offsetTimeline.GetEventtimeFromInt64(o)
		}
	}

	if epoch == math.MaxInt64 {
		return processor.Watermark(time.Time{})
	}

	return processor.Watermark(time.Unix(epoch, 0))
}

// GetWatermark gets the smallest timestamp for the given offset
func (e *Edge) GetWatermark(inputOffset isb.Offset) processor.Watermark {
	var offset, err = inputOffset.Sequence()
	if err != nil {
		e.log.Errorw("unable to get offset from isb.Offset.Sequence()", zap.Error(err))
		return processor.Watermark(time.Time{})
	}
	var debugString strings.Builder
	var epoch int64 = math.MaxInt64
	var allProcessors = e.fromVertex.GetAllProcessors()
	for _, p := range allProcessors {
		debugString.WriteString(fmt.Sprintf("[HB:%s OT:%s] %s\n", e.fromVertex.hbWatcher.GetKVName(), e.fromVertex.otWatcher.GetKVName(), p))
		var t = p.offsetTimeline.GetEventTime(inputOffset)
		if t != -1 && t < epoch {
			epoch = t
		}
		// TODO: can we delete an inactive processor?
		if p.IsDeleted() && (offset > p.offsetTimeline.GetHeadOffset()) {
			// if the pod is not active and the current offset is ahead of all offsets in Timeline
			e.fromVertex.DeleteProcessor(p.entity.GetID())
			e.fromVertex.heartbeat.Delete(p.entity.GetID())
		}
	}
	// if the offset is smaller than every offset in the timeline, set the value to be -1
	if epoch == math.MaxInt64 {
		epoch = -1
	}
	// TODO: use log instead of fmt.Printf
	fmt.Printf("\n%s[%s] get watermark for offset %d: %+v\n", debugString.String(), e.edgeName, offset, epoch)
	if epoch == -1 {
		return processor.Watermark(time.Time{})
	}

	return processor.Watermark(time.Unix(epoch, 0))
}

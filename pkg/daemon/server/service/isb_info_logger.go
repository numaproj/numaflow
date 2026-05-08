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

package service

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// atCapacityThresholdPct marks a buffer as "at capacity" when current
// usage is at or above this percentage of the configured limit. 95 is
// chosen so the flag rises slightly before retention-driven eviction
// becomes likely, giving operators a precondition signal.
const atCapacityThresholdPct = 95

// logISBSnapshot emits a single structured INFO log line describing the
// state of every ISB stream/consumer used by the pipeline. It is invoked
// from the daemon's health-checker tick loop on every periodic tick and
// once more (with event="shutdown_snapshot") when the daemon is shutting
// down. The fields and naming are documented in
// docs/superpowers/specs/2026-05-07-daemon-isb-info-logging-design.md
// and extended in
// docs/superpowers/specs/2026-05-07-isb-retention-drop-detection-design.md.
//
// In addition to the periodic snapshot, this function detects
// retention-driven message loss by comparing each buffer's current
// StreamFirstSeq / ConsumerAckFloorStreamSeq against the previous
// tick's values held in prevSnapshot. When StreamFirstSeq has advanced
// past the previous tick's AckFloor, JetStream has evicted unacked
// messages from the head of the stream — the silent-loss path under
// LimitsPolicy + DiscardOld. A separate WARN-level
// "ISB retention drop detected" log is emitted in that case so it
// surfaces on Splunk severity dashboards even if the periodic INFO log
// is filtered out.
//
// prevSnapshot is keyed by buffer name and is updated AFTER the
// snapshot is emitted, so each tick has the previous tick's state to
// diff against. The first tick after daemon startup has no prev entry
// for any buffer and emits no retention-drop logs (no false
// positives). prevSnapshotLock guards prevSnapshot for the duration of
// this function — at most one snapshot is in flight at a time, but the
// lock makes that explicit and is cheap.
func logISBSnapshot(
	ctx context.Context,
	pipeline *v1alpha1.Pipeline,
	isbSvcType v1alpha1.ISBSvcType,
	event string,
	buffers []*isbsvc.BufferInfo,
	prevSnapshot map[string]*snapshotState,
	prevSnapshotLock *sync.Mutex,
) {
	log := logging.FromContext(ctx)

	prevSnapshotLock.Lock()
	defer prevSnapshotLock.Unlock()

	bufferEntries := make([]any, 0, len(buffers))
	for _, b := range buffers {
		vertexName := ""
		if v := pipeline.FindVertexWithBuffer(b.Name); v != nil {
			vertexName = v.Name
		}
		// Under the default JetStream retention (LimitsPolicy + DiscardOld),
		// messages can be evicted from the stream by MaxMsgs/MaxBytes/MaxAge
		// without ever being acked. Those evictions do not advance AckFloor,
		// so this value will include evicted-and-thus-lost messages — which
		// is exactly what makes it useful for message-loss debugging, but
		// means it is not strictly the "currently buffered" count.
		inflight := b.StreamLastSeq - b.ConsumerAckFloorStreamSeq
		if inflight < 0 {
			inflight = 0
		}
		// undeliveredLag is the pure backlog awaiting first delivery to
		// the consumer (LastSeq - DeliveredStreamSeq). Distinct from
		// inflight which also includes delivered-but-unacked messages.
		undeliveredLag := b.StreamLastSeq - b.ConsumerDeliveredStreamSeq
		if undeliveredLag < 0 {
			undeliveredLag = 0
		}
		bufferEntries = append(bufferEntries, map[string]any{
			"buffer":                b.Name,
			"vertex":                vertexName,
			"stream_msgs":           b.StreamMsgs,
			"stream_first_seq":      b.StreamFirstSeq,
			"stream_last_seq":       b.StreamLastSeq,
			"stream_bytes":          b.StreamBytes,
			"pending":               b.PendingCount,
			"ack_pending":           b.AckPendingCount,
			"num_redelivered":       b.ConsumerNumRedelivered,
			"num_waiting":           b.ConsumerNumWaiting,
			"delivered_stream_seq":  b.ConsumerDeliveredStreamSeq,
			"ack_floor_stream_seq":  b.ConsumerAckFloorStreamSeq,
			"inflight":              inflight,
			"undelivered_lag":       undeliveredLag,
			"stream_retention":      b.StreamRetention,
			"stream_discard":        b.StreamDiscard,
			"stream_max_msgs":       b.StreamMaxMsgs,
			"stream_max_bytes":      b.StreamMaxBytes,
			"stream_max_age_sec":    b.StreamMaxAgeSec,
			"stream_duplicates_sec": b.StreamDuplicatesSec,
			"capacity_pct_msgs":     capacityPct(b.StreamMsgs, b.StreamMaxMsgs),
			"capacity_pct_bytes":    capacityPct(b.StreamBytes, b.StreamMaxBytes),
			"at_capacity":           atCapacity(b),
		})

		// Retention-drop detection. We only emit when both:
		//   1. StreamFirstSeq advanced since the previous tick (eviction
		//      occurred), AND
		//   2. the new StreamFirstSeq is past prev AckFloor + 1 (the
		//      evicted seqs were not all already-acked at last tick).
		// Note on accuracy: between two ticks the consumer's AckFloor
		// may have advanced past some of the evicted seqs before they
		// were evicted. We only have AckFloor snapshots at tick
		// boundaries, so evicted_unacked_estimate is an *upper bound*.
		// Actual loss is <= evicted_unacked_estimate.
		if prev, ok := prevSnapshot[b.Name]; ok {
			if b.StreamFirstSeq > prev.streamFirstSeq {
				evictedTotal := b.StreamFirstSeq - prev.streamFirstSeq
				evictedUnacked := b.StreamFirstSeq - prev.consumerAckFloorStreamSeq - 1
				if evictedUnacked > evictedTotal {
					evictedUnacked = evictedTotal
				}
				if evictedUnacked > 0 {
					log.Warnw("ISB retention drop detected",
						zap.String("pipeline", pipeline.Name),
						zap.String("buffer", b.Name),
						zap.String("vertex", vertexName),
						zap.String("stream_retention", b.StreamRetention),
						zap.String("stream_discard", b.StreamDiscard),
						zap.Int64("stream_max_msgs", b.StreamMaxMsgs),
						zap.Int64("stream_max_bytes", b.StreamMaxBytes),
						zap.Int64("stream_max_age_sec", b.StreamMaxAgeSec),
						zap.Int64("first_seq_before", prev.streamFirstSeq),
						zap.Int64("first_seq_after", b.StreamFirstSeq),
						zap.Int64("ack_floor_at_prev_tick", prev.consumerAckFloorStreamSeq),
						zap.Int64("evicted_total", evictedTotal),
						zap.Int64("evicted_unacked_estimate", evictedUnacked),
					)
				}
			}
		}

		// Update prev state for next tick. Allocates one snapshotState
		// per buffer per pipeline; ~24 bytes each, harmless.
		prevSnapshot[b.Name] = &snapshotState{
			streamFirstSeq:            b.StreamFirstSeq,
			consumerAckFloorStreamSeq: b.ConsumerAckFloorStreamSeq,
			streamLastSeq:             b.StreamLastSeq,
		}
	}

	log.Infow("ISB snapshot",
		zap.String("pipeline", pipeline.Name),
		zap.String("isb_svc_type", string(isbSvcType)),
		zap.String("event", event),
		zap.Int("num_buffers", len(buffers)),
		zap.Any("buffers", bufferEntries),
	)
}

// capacityPct returns the integer percentage 0..100 of `used` against
// `max`. Returns -1 when `max <= 0` (which is how nats.go represents
// "unlimited" for MaxMsgs and MaxBytes), so dashboards can distinguish
// "no limit" from "0% used". The result is clamped to [0, 100] because
// stream usage can momentarily exceed the limit during the publish
// that triggers eviction.
func capacityPct(used, max int64) int64 {
	if max <= 0 {
		return -1
	}
	pct := used * 100 / max
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// atCapacity returns true when either the message-count or byte-size
// limit is at >= atCapacityThresholdPct of capacity. Each limit is
// ignored when its configured max is <= 0 (unlimited). Used as a
// fast-path boolean for Splunk dashboards that want to alert before a
// retention drop occurs.
func atCapacity(b *isbsvc.BufferInfo) bool {
	if b.StreamMaxMsgs > 0 && b.StreamMsgs*100 >= b.StreamMaxMsgs*atCapacityThresholdPct {
		return true
	}
	if b.StreamMaxBytes > 0 && b.StreamBytes*100 >= b.StreamMaxBytes*atCapacityThresholdPct {
		return true
	}
	return false
}

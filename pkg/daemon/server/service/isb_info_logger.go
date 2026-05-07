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

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// logISBSnapshot emits a single structured INFO log line describing the
// state of every ISB stream/consumer used by the pipeline. It is invoked
// from the daemon's health-checker tick loop on every periodic tick and
// once more (with event="shutdown_snapshot") when the daemon is shutting
// down. The fields and naming are documented in
// docs/superpowers/specs/2026-05-07-daemon-isb-info-logging-design.md.
func logISBSnapshot(
	ctx context.Context,
	pipeline *v1alpha1.Pipeline,
	isbSvcType v1alpha1.ISBSvcType,
	event string,
	buffers []*isbsvc.BufferInfo,
) {
	log := logging.FromContext(ctx)

	bufferEntries := make([]any, 0, len(buffers))
	for _, b := range buffers {
		vertexName := ""
		if v := pipeline.FindVertexWithBuffer(b.Name); v != nil {
			vertexName = v.Name
		}
		inflight := b.StreamLastSeq - b.ConsumerAckFloorStreamSeq
		if inflight < 0 {
			inflight = 0
		}
		bufferEntries = append(bufferEntries, map[string]any{
			"buffer":               b.Name,
			"vertex":               vertexName,
			"stream_msgs":          b.StreamMsgs,
			"stream_first_seq":     b.StreamFirstSeq,
			"stream_last_seq":      b.StreamLastSeq,
			"stream_bytes":         b.StreamBytes,
			"pending":              b.PendingCount,
			"ack_pending":          b.AckPendingCount,
			"num_redelivered":      b.ConsumerNumRedelivered,
			"num_waiting":          b.ConsumerNumWaiting,
			"delivered_stream_seq": b.ConsumerDeliveredStreamSeq,
			"ack_floor_stream_seq": b.ConsumerAckFloorStreamSeq,
			"inflight":             inflight,
		})
	}

	log.Infow("ISB snapshot",
		zap.String("pipeline", pipeline.Name),
		zap.String("isb_svc_type", string(isbSvcType)),
		zap.String("event", event),
		zap.Int("num_buffers", len(buffers)),
		zap.Any("buffers", bufferEntries),
	)
}

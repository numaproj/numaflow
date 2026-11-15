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

package server

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
)

// edgeWatermarkMetrics exposes the watermark of every edge buffer partition as a gauge.
//
// Edges are labeled by from and to vertex, because the to vertex alone doesn't identify an edge
// for joins and cycles. The partition is labeled with the buffer name, the same partition_name
// used by the per-buffer vertex metrics, so the two can be joined.
type edgeWatermarkMetrics struct {
	namespace string
	pipeline  string
	gauge     *prometheus.GaugeVec
	// label values of the series set by the previous update, used to drop series for
	// edges that are no longer reported (e.g. their watermark fetch failed).
	exposed map[[3]string]struct{}
}

func newEdgeWatermarkMetrics(pl *v1alpha1.Pipeline, gauge *prometheus.GaugeVec) *edgeWatermarkMetrics {
	return &edgeWatermarkMetrics{
		namespace: pl.Namespace,
		pipeline:  pl.Name,
		gauge:     gauge,
		exposed:   make(map[[3]string]struct{}),
	}
}

// update sets one series per edge partition from the watermarks returned by the daemon's
// watermark service. Series that are not part of this update are deleted, so a failed fetch
// shows up as a missing series rather than a stale value.
func (m *edgeWatermarkMetrics) update(watermarks []*daemon.EdgeWatermark) {
	current := make(map[[3]string]struct{})
	for _, edge := range watermarks {
		// with watermarks disabled the service returns time zero for every partition, which
		// isn't a real watermark
		if !edge.GetIsWatermarkEnabled().GetValue() {
			continue
		}
		for idx, wm := range edge.GetWatermarks() {
			key := [3]string{edge.GetFrom(), edge.GetTo(), v1alpha1.GenerateBufferName(m.namespace, m.pipeline, edge.GetTo(), idx)}
			m.gauge.WithLabelValues(m.pipeline, key[0], key[1], key[2]).Set(float64(wm.GetValue()))
			current[key] = struct{}{}
		}
	}
	for key := range m.exposed {
		if _, ok := current[key]; !ok {
			m.gauge.DeleteLabelValues(m.pipeline, key[0], key[1], key[2])
		}
	}
	m.exposed = current
}

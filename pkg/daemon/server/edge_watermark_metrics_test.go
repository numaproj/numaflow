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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/metrics"
)

func newTestEdgeWatermarkMetrics() (*edgeWatermarkMetrics, *prometheus.GaugeVec) {
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "edge_watermark",
	}, []string{metrics.LabelPipeline, metrics.LabelFromVertex, metrics.LabelToVertex, metrics.LabelPartitionName})
	pl := &v1alpha1.Pipeline{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pl"}}
	return newEdgeWatermarkMetrics(pl, gauge), gauge
}

// seriesCount returns the number of series the gauge currently exposes.
func seriesCount(gauge *prometheus.GaugeVec) int {
	ch := make(chan prometheus.Metric, 64)
	gauge.Collect(ch)
	close(ch)
	return len(ch)
}

// value returns the current value of the series with the given label values.
func value(t *testing.T, gauge *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	g, err := gauge.GetMetricWithLabelValues(labels...)
	assert.NoError(t, err)
	var m dto.Metric
	assert.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}

func edgeWatermark(from, to string, enabled bool, wms ...int64) *daemon.EdgeWatermark {
	values := make([]*wrapperspb.Int64Value, len(wms))
	for i, wm := range wms {
		values[i] = wrapperspb.Int64(wm)
	}
	return &daemon.EdgeWatermark{
		Pipeline:           "pl",
		Edge:               from + "-" + to,
		From:               from,
		To:                 to,
		Watermarks:         values,
		IsWatermarkEnabled: wrapperspb.Bool(enabled),
	}
}

func TestEdgeWatermarkMetrics_JoinEdgesAreSeparateSeries(t *testing.T) {
	m, gauge := newTestEdgeWatermarkMetrics()

	// two sources joining into one vertex with 2 partitions
	m.update([]*daemon.EdgeWatermark{
		edgeWatermark("in-a", "join", true, 1000, 1001),
		edgeWatermark("in-b", "join", true, 2000, -1),
	})

	assert.Equal(t, 4, seriesCount(gauge))
	assert.Equal(t, float64(1000), value(t, gauge, "pl", "in-a", "join", "ns-pl-join-0"))
	assert.Equal(t, float64(1001), value(t, gauge, "pl", "in-a", "join", "ns-pl-join-1"))
	assert.Equal(t, float64(2000), value(t, gauge, "pl", "in-b", "join", "ns-pl-join-0"))
	// no watermark yet is passed through as -1
	assert.Equal(t, float64(-1), value(t, gauge, "pl", "in-b", "join", "ns-pl-join-1"))
}

func TestEdgeWatermarkMetrics_UpdatesValues(t *testing.T) {
	m, gauge := newTestEdgeWatermarkMetrics()

	m.update([]*daemon.EdgeWatermark{edgeWatermark("in", "out", true, 1000)})
	m.update([]*daemon.EdgeWatermark{edgeWatermark("in", "out", true, 5000)})

	assert.Equal(t, 1, seriesCount(gauge))
	assert.Equal(t, float64(5000), value(t, gauge, "pl", "in", "out", "ns-pl-out-0"))
}

func TestEdgeWatermarkMetrics_DropsEdgesMissingFromUpdate(t *testing.T) {
	m, gauge := newTestEdgeWatermarkMetrics()

	m.update([]*daemon.EdgeWatermark{
		edgeWatermark("in", "map", true, 1000),
		edgeWatermark("map", "out", true, 900),
	})
	assert.Equal(t, 2, seriesCount(gauge))

	// the watermark service leaves out an edge whose fetch failed, its old value must not linger
	m.update([]*daemon.EdgeWatermark{edgeWatermark("in", "map", true, 1100)})

	assert.Equal(t, 1, seriesCount(gauge))
	assert.Equal(t, float64(1100), value(t, gauge, "pl", "in", "map", "ns-pl-map-0"))
}

func TestEdgeWatermarkMetrics_SkipsDisabledWatermarks(t *testing.T) {
	m, gauge := newTestEdgeWatermarkMetrics()

	m.update([]*daemon.EdgeWatermark{edgeWatermark("in", "out", false, 0, 0)})

	assert.Equal(t, 0, seriesCount(gauge))
}

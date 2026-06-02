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

package v1

import (
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

// This file holds the DTOs for the discovery (topology) and correlation
// (debug-snapshot) endpoints, plus the typed wrappers for raw daemon responses.

// PipelineTopologyDTO is the discovery view of a pipeline's graph: its vertices
// (with roles, partitions, scale bounds, and expected container names) and edges.
// It is derived purely from the spec and carries no secret-bearing config.
type PipelineTopologyDTO struct {
	Pipeline string              `json:"pipeline"`
	Vertices []TopologyVertexDTO `json:"vertices"`
	Edges    []TopologyEdgeDTO   `json:"edges"`
}

// TopologyVertexDTO describes a pipeline vertex in the topology response.
type TopologyVertexDTO struct {
	Name string `json:"name"`
	// Type is one of: source, sink, map-udf, reduce-udf.
	Type       string `json:"type"`
	Partitions int    `json:"partitions"`
	ScaleMin   int32  `json:"scaleMin"`
	ScaleMax   int32  `json:"scaleMax"`
	// ExpectedContainers are the container names this vertex's pods should run,
	// derived from the spec (not observed from live pods).
	ExpectedContainers []string `json:"expectedContainers"`
}

// TopologyEdgeDTO describes a directed edge in the pipeline topology response.
type TopologyEdgeDTO struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// vertexRole classifies an AbstractVertex for the topology DTO.
func vertexRole(v v1alpha1.AbstractVertex) string {
	switch {
	case v.IsASource():
		return "source"
	case v.IsASink():
		return "sink"
	case v.IsReduceUDF():
		return "reduce-udf"
	default:
		return "map-udf"
	}
}

// expectedContainers returns the container names a vertex's pods are expected to
// run, derived from the spec. Every vertex has the main and monitor containers;
// role-specific user-defined containers are added based on what the spec configures.
func expectedContainers(v v1alpha1.AbstractVertex) []string {
	names := []string{v1alpha1.CtrMain, v1alpha1.CtrMonitor}
	switch {
	case v.IsASource():
		if v.IsUDSource() {
			names = append(names, v1alpha1.CtrUdsource)
		}
		if v.HasUDTransformer() {
			names = append(names, v1alpha1.CtrUdtransformer)
		}
	case v.IsASink():
		if v.IsUDSink() {
			names = append(names, v1alpha1.CtrUdsink)
		}
		if v.HasFallbackUDSink() {
			names = append(names, v1alpha1.CtrFallbackUdsink)
		}
		if v.HasOnSuccessUDSink() {
			names = append(names, v1alpha1.CtrOnSuccessUdsink)
		}
	default: // udf
		names = append(names, v1alpha1.CtrUdf)
	}
	// Vertices with side inputs also run the side-inputs synchronizer sidecar.
	if len(v.SideInputs) > 0 {
		names = append(names, v1alpha1.CtrSideInputsWatcher)
	}
	return names
}

func newPipelineTopologyDTO(pl *v1alpha1.Pipeline) PipelineTopologyDTO {
	dto := PipelineTopologyDTO{Pipeline: pl.Name}
	for _, v := range pl.Spec.Vertices {
		dto.Vertices = append(dto.Vertices, TopologyVertexDTO{
			Name:               v.Name,
			Type:               vertexRole(v),
			Partitions:         pl.NumOfPartitions(v.Name),
			ScaleMin:           v.Scale.GetMinReplicas(),
			ScaleMax:           v.Scale.GetMaxReplicas(),
			ExpectedContainers: expectedContainers(v),
		})
	}
	for _, e := range pl.Spec.Edges {
		dto.Edges = append(dto.Edges, TopologyEdgeDTO{From: e.From, To: e.To})
	}
	return dto
}

// EdgeWatermarkDTO is a typed projection of a pipeline edge's watermarks (per
// partition, in epoch milliseconds).
type EdgeWatermarkDTO struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	Watermarks []int64 `json:"watermarks"`
	IsEnabled  bool    `json:"isEnabled"`
}

func newEdgeWatermarkDTOs(watermarks []*daemon.EdgeWatermark) []EdgeWatermarkDTO {
	out := make([]EdgeWatermarkDTO, 0, len(watermarks))
	for _, w := range watermarks {
		if w == nil {
			continue
		}
		wms := make([]int64, 0, len(w.GetWatermarks()))
		for _, v := range w.GetWatermarks() {
			wms = append(wms, v.GetValue())
		}
		out = append(out, EdgeWatermarkDTO{
			From:       w.GetFrom(),
			To:         w.GetTo(),
			Watermarks: wms,
			IsEnabled:  w.GetIsWatermarkEnabled().GetValue(),
		})
	}
	return out
}

// MonoVertexMetricsDTO is the typed processing rates and pending counts for a
// mono vertex, keyed by lookback window ("1m", "5m", "15m", "default").
type MonoVertexMetricsDTO struct {
	MonoVertex      string             `json:"monoVertex"`
	ProcessingRates map[string]float64 `json:"processingRates,omitempty"`
	Pending         map[string]int64   `json:"pending,omitempty"`
}

func newMonoVertexMetricsDTO(monoVertex string, m *mvtxdaemon.MonoVertexMetrics) MonoVertexMetricsDTO {
	dto := MonoVertexMetricsDTO{MonoVertex: monoVertex}
	if m == nil {
		return dto
	}
	if len(m.GetProcessingRates()) > 0 {
		dto.ProcessingRates = make(map[string]float64, len(m.GetProcessingRates()))
		for k, v := range m.GetProcessingRates() {
			dto.ProcessingRates[k] = v.GetValue()
		}
	}
	if len(m.GetPendings()) > 0 {
		dto.Pending = make(map[string]int64, len(m.GetPendings()))
		for k, v := range m.GetPendings() {
			dto.Pending[k] = v.GetValue()
		}
	}
	return dto
}

// Stable error codes for a debug-snapshot section that failed to populate.
const (
	SnapshotErrCodeDaemonUnavailable = "daemon_unavailable"
	SnapshotErrCodeTimeout           = "timeout"
	SnapshotErrCodeNotFound          = "not_found"
	SnapshotErrCodeInternal          = "internal"
)

// SnapshotSectionError is the structured error attached to a snapshot section when its
// underlying fetch failed. Agents can branch on Code.
type SnapshotSectionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// DebugSnapshotDTO is a bounded, read-only correlation of a pipeline's current
// state, composed from existing single-purpose data sources. Each section is
// independently fetched; a failed section carries an error rather than failing
// the whole response.
type DebugSnapshotDTO struct {
	Pipeline       string                        `json:"pipeline"`
	Namespace      string                        `json:"namespace"`
	ObservedAt     string                        `json:"observedAt"`
	ResourceHealth SnapshotResourceHealthSection `json:"resourceHealth"`
	DataHealth     SnapshotDataHealthSection     `json:"dataHealth"`
	WatermarkLag   SnapshotWatermarkLagSection   `json:"watermarkLag"`
	Vertices       SnapshotVertexMetricsSection  `json:"vertices"`
	Buffers        SnapshotBuffersSection        `json:"buffers"`
	RuntimeErrors  SnapshotRuntimeErrorsSection  `json:"runtimeErrors"`
	WarningEvents  SnapshotEventsSection         `json:"warningEvents"`
	Notes          []string                      `json:"notes,omitempty"`
}

// MonoVertexDebugSnapshotDTO is a bounded, read-only correlation of a mono
// vertex's current state. It mirrors DebugSnapshotDTO where mono-vertex data
// sources have parity with pipeline data sources.
type MonoVertexDebugSnapshotDTO struct {
	MonoVertex     string                                 `json:"monoVertex"`
	Namespace      string                                 `json:"namespace"`
	ObservedAt     string                                 `json:"observedAt"`
	ResourceHealth SnapshotResourceHealthSection          `json:"resourceHealth"`
	DataHealth     SnapshotDataHealthSection              `json:"dataHealth"`
	Metrics        SnapshotMonoVertexMetricsSection       `json:"metrics"`
	RuntimeErrors  SnapshotMonoVertexRuntimeErrorsSection `json:"runtimeErrors"`
	WarningEvents  SnapshotEventsSection                  `json:"warningEvents"`
	Notes          []string                               `json:"notes,omitempty"`
}

// SnapshotResourceHealthSection carries the pipeline resource health portion of a debug snapshot.
type SnapshotResourceHealthSection struct {
	Data       *ResourceHealthData   `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// ResourceHealthData is the resource-level health summary of a pipeline.
type ResourceHealthData struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// SnapshotDataHealthSection carries daemon-reported data health in a debug snapshot.
type SnapshotDataHealthSection struct {
	Data       *DataHealthDTO        `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// SnapshotWatermarkLagSection carries end-to-end watermark lag in a debug snapshot.
type SnapshotWatermarkLagSection struct {
	Data       *WatermarkLagDTO      `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// SnapshotVertexMetrics carries per-vertex pending and rates within the snapshot.
type SnapshotVertexMetrics struct {
	Vertex     string                  `json:"vertex"`
	Partitions []VertexPartitionMetric `json:"partitions"`
}

// SnapshotVertexMetricsSection carries per-vertex metrics in a debug snapshot.
type SnapshotVertexMetricsSection struct {
	Data       []SnapshotVertexMetrics `json:"data,omitempty"`
	Error      *SnapshotSectionError   `json:"error,omitempty"`
	ObservedAt string                  `json:"observedAt"`
}

// SnapshotBuffersSection carries inter-step buffer metrics in a debug snapshot.
type SnapshotBuffersSection struct {
	Data       []BufferDTO           `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// SnapshotMonoVertexMetricsSection carries mono-vertex metrics in a debug snapshot.
type SnapshotMonoVertexMetricsSection struct {
	Data       *MonoVertexMetricsDTO `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// SnapshotVertexErrors groups runtime errors under their vertex.
type SnapshotVertexErrors struct {
	Vertex string            `json:"vertex"`
	Errors []ReplicaErrorDTO `json:"errors,omitempty"`
}

// SnapshotRuntimeErrorsSection carries runtime errors in a debug snapshot.
type SnapshotRuntimeErrorsSection struct {
	Data       []SnapshotVertexErrors `json:"data,omitempty"`
	Error      *SnapshotSectionError  `json:"error,omitempty"`
	ObservedAt string                 `json:"observedAt"`
}

// SnapshotMonoVertexRuntimeErrorsSection carries mono-vertex runtime errors in a debug snapshot.
type SnapshotMonoVertexRuntimeErrorsSection struct {
	Data       []ReplicaErrorDTO     `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

// SnapshotEventsSection carries recent Kubernetes events in a debug snapshot.
type SnapshotEventsSection struct {
	Data       []K8sEventsResponse   `json:"data,omitempty"`
	Error      *SnapshotSectionError `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

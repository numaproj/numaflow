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
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

// This file holds the response DTOs for the single-purpose endpoints. They use
// bare scalars instead of protobuf wrapper types (which serialize as
// {"value":"123"}), format timestamps as RFC3339, and carry only the data the
// endpoint is named for. They are returned directly, without the
// NumaflowAPIResponse envelope.

// ConditionDTO is a projection of metav1.Condition.
type ConditionDTO struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

func newConditionDTOs(conditions []metav1.Condition) []ConditionDTO {
	if len(conditions) == 0 {
		return nil
	}
	out := make([]ConditionDTO, 0, len(conditions))
	for _, c := range conditions {
		out = append(out, ConditionDTO{
			Type:    c.Type,
			Status:  string(c.Status),
			Reason:  c.Reason,
			Message: c.Message,
		})
	}
	return out
}

// rfc3339 formats a metav1.Time as an RFC3339 string, returning "" when unset.
func rfc3339(t metav1.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// DataHealthDTO is the data-plane (criticality) health of a pipeline or mono
// vertex. status is one of: healthy, warning, critical, unknown.
type DataHealthDTO struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// VertexMetricsDTO holds the processing rates and pending counts for a single
// vertex, broken down per partition. The daemon returns one entry per partition
// in buffer order; Partition is that positional index. The rate/pending maps are
// keyed by lookback window ("1m", "5m", "15m", "default").
type VertexMetricsDTO struct {
	Pipeline   string                  `json:"pipeline"`
	Vertex     string                  `json:"vertex"`
	Partitions []VertexPartitionMetric `json:"partitions"`
}

type VertexPartitionMetric struct {
	Partition       int                `json:"partition"`
	ProcessingRates map[string]float64 `json:"processingRates,omitempty"`
	Pending         map[string]int64   `json:"pending,omitempty"`
}

func newVertexMetricsDTO(pipeline, vertex string, metrics []*daemon.VertexMetrics) VertexMetricsDTO {
	dto := VertexMetricsDTO{Pipeline: pipeline, Vertex: vertex}
	for i, m := range metrics {
		if m == nil {
			continue
		}
		part := VertexPartitionMetric{Partition: i}
		if len(m.GetProcessingRates()) > 0 {
			part.ProcessingRates = make(map[string]float64, len(m.GetProcessingRates()))
			for k, v := range m.GetProcessingRates() {
				part.ProcessingRates[k] = v.GetValue()
			}
		}
		if len(m.GetPendings()) > 0 {
			part.Pending = make(map[string]int64, len(m.GetPendings()))
			for k, v := range m.GetPendings() {
				part.Pending[k] = v.GetValue()
			}
		}
		dto.Partitions = append(dto.Partitions, part)
	}
	return dto
}

// PendingDTO holds the pending (backlog) message counts for a vertex or mono
// vertex, broken down per partition so multi-partition vertices are not
// collapsed. Each partition's counts are keyed by lookback window ("1m", "5m",
// "15m", "default").
type PendingDTO struct {
	Pipeline   string             `json:"pipeline,omitempty"`
	Vertex     string             `json:"vertex,omitempty"`
	MonoVertex string             `json:"monoVertex,omitempty"`
	Partitions []PartitionPending `json:"partitions"`
}

type PartitionPending struct {
	Partition int              `json:"partition"`
	Pending   map[string]int64 `json:"pending,omitempty"`
}

func newVertexPendingDTO(pipeline, vertex string, metrics []*daemon.VertexMetrics) PendingDTO {
	dto := PendingDTO{Pipeline: pipeline, Vertex: vertex}
	for i, m := range metrics {
		if m == nil {
			continue
		}
		pp := PartitionPending{Partition: i}
		if len(m.GetPendings()) > 0 {
			pp.Pending = make(map[string]int64, len(m.GetPendings()))
			for k, v := range m.GetPendings() {
				pp.Pending[k] = v.GetValue()
			}
		}
		dto.Partitions = append(dto.Partitions, pp)
	}
	return dto
}

func newMonoVertexPendingDTO(monoVertex string, metrics *mvtxdaemon.MonoVertexMetrics) PendingDTO {
	// A mono vertex has a single partition.
	pp := PartitionPending{Partition: 0}
	if metrics != nil && len(metrics.GetPendings()) > 0 {
		pp.Pending = make(map[string]int64, len(metrics.GetPendings()))
		for k, v := range metrics.GetPendings() {
			pp.Pending[k] = v.GetValue()
		}
	}
	return PendingDTO{MonoVertex: monoVertex, Partitions: []PartitionPending{pp}}
}

// BufferDTO is the metrics for a single inter-step buffer: how many messages are
// pending/unacked, total depth, and how full it is.
type BufferDTO struct {
	Name             string  `json:"name"`
	Pending          int64   `json:"pending"`
	AckPending       int64   `json:"ackPending"`
	TotalMessages    int64   `json:"totalMessages"`
	BufferLength     int64   `json:"bufferLength"`
	BufferUsageLimit float64 `json:"bufferUsageLimit"`
	BufferUsage      float64 `json:"bufferUsage"`
	IsFull           bool    `json:"isFull"`
}

func newBufferDTO(b *daemon.BufferInfo) BufferDTO {
	return BufferDTO{
		Name:             b.GetBufferName(),
		Pending:          b.GetPendingCount().GetValue(),
		AckPending:       b.GetAckPendingCount().GetValue(),
		TotalMessages:    b.GetTotalMessages().GetValue(),
		BufferLength:     b.GetBufferLength().GetValue(),
		BufferUsageLimit: b.GetBufferUsageLimit().GetValue(),
		BufferUsage:      b.GetBufferUsage().GetValue(),
		IsFull:           b.GetIsFull().GetValue(),
	}
}

// BufferPendingDTO holds the pending (backlog) message counts for a single
// inter-step buffer.
type BufferPendingDTO struct {
	Name       string `json:"name"`
	Pending    int64  `json:"pending"`
	AckPending int64  `json:"ackPending"`
}

func newBufferPendingDTO(b *daemon.BufferInfo) BufferPendingDTO {
	return BufferPendingDTO{
		Name:       b.GetBufferName(),
		Pending:    b.GetPendingCount().GetValue(),
		AckPending: b.GetAckPendingCount().GetValue(),
	}
}

// PipelineStatusDTO is the status subresource of a pipeline, without the spec.
type PipelineStatusDTO struct {
	Phase              string         `json:"phase"`
	Message            string         `json:"message,omitempty"`
	LastUpdated        string         `json:"lastUpdated,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration"`
	DrainedOnPause     bool           `json:"drainedOnPause"`
	VertexCount        *uint32        `json:"vertexCount,omitempty"`
	SourceCount        *uint32        `json:"sourceCount,omitempty"`
	SinkCount          *uint32        `json:"sinkCount,omitempty"`
	UDFCount           *uint32        `json:"udfCount,omitempty"`
	Conditions         []ConditionDTO `json:"conditions,omitempty"`
}

func newPipelineStatusDTO(pl *v1alpha1.Pipeline) PipelineStatusDTO {
	s := pl.Status
	return PipelineStatusDTO{
		Phase:              string(s.Phase),
		Message:            s.Message,
		LastUpdated:        rfc3339(s.LastUpdated),
		ObservedGeneration: s.ObservedGeneration,
		DrainedOnPause:     s.DrainedOnPause,
		VertexCount:        s.VertexCount,
		SourceCount:        s.SourceCount,
		SinkCount:          s.SinkCount,
		UDFCount:           s.UDFCount,
		Conditions:         newConditionDTOs(s.Conditions),
	}
}

// VertexStatusDTO is the status subresource of a single vertex, without the spec.
type VertexStatusDTO struct {
	Phase              string         `json:"phase"`
	Reason             string         `json:"reason,omitempty"`
	Message            string         `json:"message,omitempty"`
	Replicas           uint32         `json:"replicas"`
	DesiredReplicas    uint32         `json:"desiredReplicas"`
	ReadyReplicas      uint32         `json:"readyReplicas"`
	UpdatedReplicas    uint32         `json:"updatedReplicas"`
	CurrentHash        string         `json:"currentHash,omitempty"`
	UpdateHash         string         `json:"updateHash,omitempty"`
	LastScaledAt       string         `json:"lastScaledAt,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration"`
	Conditions         []ConditionDTO `json:"conditions,omitempty"`
}

func newVertexStatusDTO(v *v1alpha1.Vertex) VertexStatusDTO {
	s := v.Status
	return VertexStatusDTO{
		Phase:              string(s.Phase),
		Reason:             s.Reason,
		Message:            s.Message,
		Replicas:           s.Replicas,
		DesiredReplicas:    s.DesiredReplicas,
		ReadyReplicas:      s.ReadyReplicas,
		UpdatedReplicas:    s.UpdatedReplicas,
		CurrentHash:        s.CurrentHash,
		UpdateHash:         s.UpdateHash,
		LastScaledAt:       rfc3339(s.LastScaledAt),
		ObservedGeneration: s.ObservedGeneration,
		Conditions:         newConditionDTOs(s.Conditions),
	}
}

// MonoVertexStatusDTO is the status subresource of a mono vertex, without the spec.
type MonoVertexStatusDTO struct {
	Phase              string         `json:"phase"`
	Reason             string         `json:"reason,omitempty"`
	Message            string         `json:"message,omitempty"`
	Replicas           uint32         `json:"replicas"`
	DesiredReplicas    uint32         `json:"desiredReplicas"`
	ReadyReplicas      uint32         `json:"readyReplicas"`
	UpdatedReplicas    uint32         `json:"updatedReplicas"`
	LastUpdated        string         `json:"lastUpdated,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration"`
	Conditions         []ConditionDTO `json:"conditions,omitempty"`
}

func newMonoVertexStatusDTO(mv *v1alpha1.MonoVertex) MonoVertexStatusDTO {
	s := mv.Status
	return MonoVertexStatusDTO{
		Phase:              string(s.Phase),
		Reason:             s.Reason,
		Message:            s.Message,
		Replicas:           s.Replicas,
		DesiredReplicas:    s.DesiredReplicas,
		ReadyReplicas:      s.ReadyReplicas,
		UpdatedReplicas:    s.UpdatedReplicas,
		LastUpdated:        rfc3339(s.LastUpdated),
		ObservedGeneration: s.ObservedGeneration,
		Conditions:         newConditionDTOs(s.Conditions),
	}
}

// WatermarkLagDTO is the end-to-end watermark lag of a pipeline in milliseconds
// (max source watermark minus min sink watermark). LagMillis is -1 when data has
// not yet reached the sink.
type WatermarkLagDTO struct {
	LagMillis          int64 `json:"lagMillis"`
	MaxSourceWatermark int64 `json:"maxSourceWatermark"`
	MinSinkWatermark   int64 `json:"minSinkWatermark"`
}

// computePipelineWatermarkLag derives the end-to-end watermark lag of a pipeline
// from its edge watermarks: the largest source watermark minus the smallest sink
// watermark. When the data has not yet reached a sink (sink watermark is -1),
// LagMillis is reported as -1.
func computePipelineWatermarkLag(pl *v1alpha1.Pipeline, watermarks []*daemon.EdgeWatermark) WatermarkLagDTO {
	source := make(map[string]bool)
	sink := make(map[string]bool)
	for _, vertex := range pl.Spec.Vertices {
		if vertex.IsASource() {
			source[vertex.Name] = true
		} else if vertex.IsASink() {
			sink[vertex.Name] = true
		}
	}

	var (
		minWM       int64 = math.MaxInt64
		maxWM       int64 = math.MinInt64
		foundSink         = false
		foundSource       = false
	)
	for _, watermark := range watermarks {
		if watermark == nil {
			continue
		}
		if _, ok := source[watermark.From]; ok {
			for _, wm := range watermark.Watermarks {
				if wm.GetValue() > maxWM {
					maxWM = wm.GetValue()
				}
				foundSource = true
			}
		}
		if _, ok := sink[watermark.To]; ok {
			for _, wm := range watermark.Watermarks {
				if wm.GetValue() < minWM {
					minWM = wm.GetValue()
				}
				foundSink = true
			}
		}
	}

	lag := int64(-1)
	if !foundSource {
		maxWM = -1
	}
	if !foundSink {
		minWM = -1
	}
	if foundSource && foundSink && minWM != -1 {
		lag = maxWM - minWM
	}
	return WatermarkLagDTO{
		LagMillis:          lag,
		MaxSourceWatermark: maxWM,
		MinSinkWatermark:   minWM,
	}
}

// ReplicaErrorDTO is the set of runtime errors observed on a single replica,
// broken down per container.
type ReplicaErrorDTO struct {
	Replica string              `json:"replica"`
	Errors  []ContainerErrorDTO `json:"errors,omitempty"`
}

type ContainerErrorDTO struct {
	Container string `json:"container"`
	Timestamp string `json:"timestamp,omitempty"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message,omitempty"`
	Details   string `json:"details,omitempty"`
}

func newReplicaErrorDTOs(replicas []*daemon.ReplicaErrors) []ReplicaErrorDTO {
	out := make([]ReplicaErrorDTO, 0, len(replicas))
	for _, r := range replicas {
		if r == nil {
			continue
		}
		re := ReplicaErrorDTO{Replica: r.GetReplica()}
		for _, ce := range r.GetContainerErrors() {
			if ce == nil {
				continue
			}
			ts := ""
			if ce.GetTimestamp() != nil {
				ts = ce.GetTimestamp().AsTime().UTC().Format(time.RFC3339)
			}
			re.Errors = append(re.Errors, ContainerErrorDTO{
				Container: ce.GetContainer(),
				Timestamp: ts,
				Code:      ce.GetCode(),
				Message:   ce.GetMessage(),
				Details:   ce.GetDetails(),
			})
		}
		out = append(out, re)
	}
	return out
}

// newMonoVertexReplicaErrorDTOs maps mono vertex daemon replica errors. The
// mvtxdaemon proto types mirror the pipeline daemon's but are distinct Go types,
// so they need their own mapper.
func newMonoVertexReplicaErrorDTOs(replicas []*mvtxdaemon.ReplicaErrors) []ReplicaErrorDTO {
	out := make([]ReplicaErrorDTO, 0, len(replicas))
	for _, r := range replicas {
		if r == nil {
			continue
		}
		re := ReplicaErrorDTO{Replica: r.GetReplica()}
		for _, ce := range r.GetContainerErrors() {
			if ce == nil {
				continue
			}
			ts := ""
			if ce.GetTimestamp() != nil {
				ts = ce.GetTimestamp().AsTime().UTC().Format(time.RFC3339)
			}
			re.Errors = append(re.Errors, ContainerErrorDTO{
				Container: ce.GetContainer(),
				Timestamp: ts,
				Code:      ce.GetCode(),
				Message:   ce.GetMessage(),
				Details:   ce.GetDetails(),
			})
		}
		out = append(out, re)
	}
	return out
}

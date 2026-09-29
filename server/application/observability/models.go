/*
Copyright 2026 The Numaproj Authors.

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

// Package observability builds transport-independent, task-oriented views of
// Numaflow runtime resources for API v2. It reads Kubernetes CRs only; it does
// not call the daemon, list Pods, or implement v1-style live resource health.
package observability

import "time"

type TargetKind string

const (
	TargetKindPipelineVertex TargetKind = "PipelineVertex"
)

type HealthState string

const (
	HealthStateHealthy  HealthState = "healthy"
	HealthStateWarning  HealthState = "warning"
	HealthStateCritical HealthState = "critical"
	HealthStateInactive HealthState = "inactive"
	HealthStateUnknown  HealthState = "unknown"
)

type TargetRef struct {
	Kind      TargetKind
	Namespace string
	Pipeline  string
	Name      string
	UID       string
}

// Health is controller-reported resource health for summary endpoints. It is not
// equivalent to v1 pipeline /health (Pod inspection + data-flow health).
type Health struct {
	State   HealthState
	Reason  string
	Message string
}

// VertexSummary is the compact projection returned by summary APIs (not a CR dump).
type VertexSummary struct {
	Ref                TargetRef
	VertexType         string
	Phase              string
	DesiredPhase       string
	Health             Health
	Generation         int64
	ObservedGeneration int64
	CreatedAt          time.Time
	ObservedAt         time.Time
	LastScaledAt       *time.Time
	// Capabilities lists follow-up actions available for this target (e.g. "summary").
	// This is separate from GET /capabilities operations (OpenAPI operationIds).
	Capabilities    []string
	TruncatedFields []string
}

// Result pairs a DTO with the backing CR resourceVersion for HTTP ETag handling.
type Result[T any] struct {
	Value           T
	ResourceVersion string
}

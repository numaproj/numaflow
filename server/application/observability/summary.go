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

package observability

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const (
	// Align with OpenAPI maxLength on health.reason / health.message for agents and UI.
	maximumReasonLength  = 256
	maximumMessageLength = 1024
)

// GetPipelineVertexSummary returns a compact summary for one pipeline vertex.
func (s *Service) GetPipelineVertexSummary(ctx context.Context, namespace, pipeline, vertex string) (Result[VertexSummary], error) {
	resource, err := s.getPipelineVertex(ctx, namespace, pipeline, vertex)
	if err != nil {
		return Result[VertexSummary]{}, err
	}

	health, truncatedFields := normalizeHealth(resource)
	return Result[VertexSummary]{
		Value: VertexSummary{
			Ref: TargetRef{
				Kind:      TargetKindPipelineVertex,
				Namespace: namespace,
				Pipeline:  pipeline,
				Name:      vertex,
				UID:       string(resource.UID),
			},
			VertexType:         string(resource.GetVertexType()),
			Phase:              string(resource.Status.Phase),
			DesiredPhase:       string(resource.Spec.Lifecycle.GetDesiredPhase()),
			Health:             health,
			Generation:         resource.Generation,
			ObservedGeneration: resource.Status.ObservedGeneration,
			CreatedAt:          resource.CreationTimestamp.UTC(),
			ObservedAt:         resourceObservedAt(resource.CreationTimestamp, resource.Status.Conditions, resource.Status.LastScaledAt),
			LastScaledAt:       optionalTime(resource.Status.LastScaledAt),
			// Status and other follow-ups are added here when those routes exist.
			Capabilities:    []string{"summary"},
			TruncatedFields: truncatedFields,
		},
		ResourceVersion: resource.ResourceVersion,
	}, nil
}

// normalizeHealth maps Vertex controller state to API v2 HealthState. Generation
// lag is surfaced as warning/Progressing so agents do not treat stale spec as healthy.
func normalizeHealth(resource *dfv1.Vertex) (Health, []string) {
	phase := resource.Status.Phase
	desiredPhase := resource.Spec.Lifecycle.GetDesiredPhase()
	reason := resource.Status.Reason
	message := resource.Status.Message

	state := HealthStateUnknown
	switch {
	case phase == dfv1.VertexPhaseFailed:
		state = HealthStateCritical
	case phase == dfv1.VertexPhasePaused || desiredPhase == dfv1.VertexPhasePaused:
		state = HealthStateInactive
	case resource.Status.ObservedGeneration == 0 || resource.Generation > resource.Status.ObservedGeneration:
		state = HealthStateWarning
		reason = "Progressing"
		message = "The Vertex controller has not observed the latest generation"
	case phase == dfv1.VertexPhaseRunning && resource.Status.IsHealthy():
		state = HealthStateHealthy
	case phase == dfv1.VertexPhaseRunning:
		state = HealthStateWarning
	}

	reason, reasonTruncated := truncateString(reason, maximumReasonLength)
	message, messageTruncated := truncateString(message, maximumMessageLength)
	truncatedFields := make([]string, 0, 2)
	if reasonTruncated {
		truncatedFields = append(truncatedFields, "health.reason")
	}
	if messageTruncated {
		truncatedFields = append(truncatedFields, "health.message")
	}
	return Health{State: state, Reason: reason, Message: message}, truncatedFields
}

func optionalTime(value metav1.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	result := value.UTC()
	return &result
}

// resourceObservedAt is the latest controller-provided timestamp (not request time).
func resourceObservedAt(createdAt metav1.Time, conditions []metav1.Condition, timestamps ...metav1.Time) time.Time {
	observedAt := createdAt.Time
	for _, timestamp := range timestamps {
		if timestamp.After(observedAt) {
			observedAt = timestamp.Time
		}
	}
	for _, condition := range conditions {
		if condition.LastTransitionTime.After(observedAt) {
			observedAt = condition.LastTransitionTime.Time
		}
	}
	return observedAt.UTC()
}

func truncateString(value string, maximumLength int) (string, bool) {
	runes := []rune(value)
	if len(runes) <= maximumLength {
		return value, false
	}
	return string(runes[:maximumLength]), true
}

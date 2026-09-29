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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	fakeclientset "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
)

var summaryObservedAt = time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)

func TestPipelineVertexSummary(t *testing.T) {
	transition := metav1.NewTime(summaryObservedAt)
	vertex := testPipelineVertex()
	vertex.ObjectMeta = metav1.ObjectMeta{
		Name:              "orders-map",
		Namespace:         "team-a",
		UID:               types.UID("vertex-uid"),
		ResourceVersion:   "17",
		Generation:        4,
		CreationTimestamp: metav1.NewTime(summaryObservedAt.Add(-time.Hour)),
	}
	vertex.Status = dfv1.VertexStatus{
		Status: dfv1.Status{Conditions: []metav1.Condition{
			{Type: string(dfv1.VertexConditionDeployed), Status: metav1.ConditionTrue, LastTransitionTime: transition},
			{Type: string(dfv1.VertexConditionPodsHealthy), Status: metav1.ConditionTrue, Reason: "Ready", Message: "All pods are ready", LastTransitionTime: transition},
		}},
		Phase:              dfv1.VertexPhaseRunning,
		ObservedGeneration: 4,
	}
	service := newTestService(t, vertex)

	summary, err := service.GetPipelineVertexSummary(context.Background(), "team-a", "orders", "map")
	require.NoError(t, err)
	assert.Equal(t, "17", summary.ResourceVersion)
	assert.Equal(t, TargetKindPipelineVertex, summary.Value.Ref.Kind)
	assert.Equal(t, "orders", summary.Value.Ref.Pipeline)
	assert.Equal(t, "map", summary.Value.Ref.Name)
	assert.Equal(t, string(dfv1.VertexTypeMapUDF), summary.Value.VertexType)
	assert.Equal(t, HealthStateHealthy, summary.Value.Health.State)
	assert.Equal(t, transition.Time, summary.Value.ObservedAt)
	assert.Equal(t, []string{"summary"}, summary.Value.Capabilities)
}

func TestPipelineVertexSummaryGenerationProgressing(t *testing.T) {
	vertex := testPipelineVertex()
	vertex.Generation = 4
	vertex.Status.Phase = dfv1.VertexPhaseRunning
	vertex.Status.ObservedGeneration = 3
	vertex.Status.Conditions = []metav1.Condition{{Type: string(dfv1.VertexConditionPodsHealthy), Status: metav1.ConditionTrue}}
	service := newTestService(t, vertex)

	summary, err := service.GetPipelineVertexSummary(context.Background(), "team-a", "orders", "map")
	require.NoError(t, err)
	assert.Equal(t, HealthStateWarning, summary.Value.Health.State)
	assert.Equal(t, "Progressing", summary.Value.Health.Reason)
}

func TestNormalizeHealthStates(t *testing.T) {
	tests := []struct {
		name               string
		phase              dfv1.VertexPhase
		desiredPhase       dfv1.VertexPhase
		generation         int64
		observedGeneration int64
		conditions         []metav1.Condition
		want               HealthState
	}{
		{
			name:               "failed is critical",
			phase:              dfv1.VertexPhaseFailed,
			desiredPhase:       dfv1.VertexPhaseRunning,
			observedGeneration: 1,
			want:               HealthStateCritical,
		},
		{
			name:               "paused is inactive",
			phase:              dfv1.VertexPhasePaused,
			desiredPhase:       dfv1.VertexPhasePaused,
			observedGeneration: 1,
			want:               HealthStateInactive,
		},
		{
			name:               "unobserved generation is warning",
			phase:              dfv1.VertexPhaseRunning,
			desiredPhase:       dfv1.VertexPhaseRunning,
			generation:         2,
			observedGeneration: 1,
			want:               HealthStateWarning,
		},
		{
			name:               "ready running vertex is healthy",
			phase:              dfv1.VertexPhaseRunning,
			desiredPhase:       dfv1.VertexPhaseRunning,
			generation:         1,
			observedGeneration: 1,
			conditions: []metav1.Condition{
				{Type: string(dfv1.VertexConditionDeployed), Status: metav1.ConditionTrue},
				{Type: string(dfv1.VertexConditionPodsHealthy), Status: metav1.ConditionTrue},
			},
			want: HealthStateHealthy,
		},
		{
			name:               "unready running vertex is warning",
			phase:              dfv1.VertexPhaseRunning,
			desiredPhase:       dfv1.VertexPhaseRunning,
			generation:         1,
			observedGeneration: 1,
			conditions:         []metav1.Condition{{Type: string(dfv1.VertexConditionPodsHealthy), Status: metav1.ConditionFalse}},
			want:               HealthStateWarning,
		},
		{
			name:               "unknown phase is unknown",
			desiredPhase:       dfv1.VertexPhaseRunning,
			observedGeneration: 1,
			want:               HealthStateUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vertex := testPipelineVertex()
			vertex.Generation = test.generation
			vertex.Status = dfv1.VertexStatus{
				Status:             dfv1.Status{Conditions: test.conditions},
				Phase:              test.phase,
				ObservedGeneration: test.observedGeneration,
			}
			health, _ := normalizeHealth(vertex)
			assert.Equal(t, test.want, health.State)
		})
	}
}

func TestPipelineVertexSummaryBoundsProviderText(t *testing.T) {
	vertex := testPipelineVertex()
	vertex.Status.Phase = dfv1.VertexPhaseFailed
	vertex.Status.Reason = strings.Repeat("r", maximumReasonLength+1)
	vertex.Status.Message = strings.Repeat("m", maximumMessageLength+1)
	service := newTestService(t, vertex)

	summary, err := service.GetPipelineVertexSummary(context.Background(), "team-a", "orders", "map")
	require.NoError(t, err)
	assert.Equal(t, HealthStateCritical, summary.Value.Health.State)
	assert.Len(t, []rune(summary.Value.Health.Reason), maximumReasonLength)
	assert.Len(t, []rune(summary.Value.Health.Message), maximumMessageLength)
	assert.ElementsMatch(t, []string{"health.reason", "health.message"}, summary.Value.TruncatedFields)
}

func TestPipelineVertexSummaryRejectsAmbiguousResourceName(t *testing.T) {
	vertex := testPipelineVertex()
	vertex.Name = "a-b-c"
	vertex.Spec.PipelineName = "a-b"
	vertex.Spec.Name = "c"
	service := newTestService(t, vertex)

	_, err := service.GetPipelineVertexSummary(context.Background(), "team-a", "a", "b-c")
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestPipelineVertexSummaryReturnsProviderError(t *testing.T) {
	clientset := fakeclientset.NewSimpleClientset()
	clientset.PrependReactor("get", "vertices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, assert.AnError
	})
	service, err := NewService(clientset.NumaflowV1alpha1())
	require.NoError(t, err)

	_, err = service.GetPipelineVertexSummary(context.Background(), "team-a", "orders", "map")
	assert.ErrorIs(t, err, assert.AnError)
}

func testPipelineVertex() *dfv1.Vertex {
	return &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-map", Namespace: "team-a"},
		Spec: dfv1.VertexSpec{
			AbstractVertex: dfv1.AbstractVertex{Name: "map", UDF: &dfv1.UDF{}},
			PipelineName:   "orders",
			Lifecycle:      dfv1.VertexLifecycle{DesiredPhase: dfv1.VertexPhaseRunning},
		},
	}
}

func newTestService(t *testing.T, objects ...runtime.Object) *Service {
	t.Helper()
	clientset := fakeclientset.NewSimpleClientset()
	clientset.PrependReactor("get", "vertices", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok {
			return false, nil, nil
		}
		for _, object := range objects {
			vertex, ok := object.(*dfv1.Vertex)
			if ok && action.GetNamespace() == vertex.Namespace && getAction.GetName() == vertex.Name {
				return true, vertex.DeepCopy(), nil
			}
		}
		return false, nil, nil
	})
	service, err := NewService(clientset.NumaflowV1alpha1())
	require.NoError(t, err)
	return service
}

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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

func TestGetCurrentHealth(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{} // Simplified for testing
	hc := NewHealthChecker(monoVertex)
	expected := defaultDataHealthResponse

	if result := hc.getCurrentHealth(); result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestSetCurrentHealth(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{} // Simplified
	hc := NewHealthChecker(monoVertex)
	newStatus := newDataHealthResponse("Healthy", "All systems green", "D1")

	hc.setCurrentHealth(newStatus)

	if result := hc.getCurrentHealth(); result != newStatus {
		t.Errorf("Expected %v, got %v", newStatus, result)
	}
}

func TestConvertMonoVtxStateToHealthResp(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{} // Simplified
	hc := NewHealthChecker(monoVertex)

	tests := []struct {
		name     string
		state    *monoVtxState
		expected *dataHealthResponse
	}{
		{
			name:     "Healthy State",
			state:    newMonoVtxState("vertex1", v1alpha1.MonoVertexStatusHealthy),
			expected: newDataHealthResponse(v1alpha1.MonoVertexStatusHealthy, "MonoVertex data flow is healthy", "D1"),
		},
		{
			name:     "Warning State",
			state:    newMonoVtxState("vertex1", v1alpha1.MonoVertexStatusWarning),
			expected: newDataHealthResponse(v1alpha1.MonoVertexStatusWarning, "MonoVertex data flow is in a warning state for vertex1", "D2"),
		},
		{
			name:     "Critical State",
			state:    newMonoVtxState("vertex1", v1alpha1.MonoVertexStatusCritical),
			expected: newDataHealthResponse(v1alpha1.MonoVertexStatusCritical, "MonoVertex data flow is in a critical state for vertex1", "D3"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := hc.convertMonoVtxStateToHealthResp(test.state)
			if result.Status != test.expected.Status || result.Message != test.expected.Message || result.Code != test.expected.Code {
				t.Errorf("Expected %+v, got %+v", test.expected, result)
			}
		})
	}
}

func TestGetDesiredReplica(t *testing.T) {
	targetProcessingSeconds := uint32(5)
	monoVertex := &v1alpha1.MonoVertex{
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{TargetProcessingSeconds: &targetProcessingSeconds},
		},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 100},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 500},
		},
	}

	result, err := hc.getDesiredReplica(metrics, 4)
	require.NoError(t, err)
	assert.Equal(t, 4, result)
}

func TestGetDesiredReplicaZeroRateUsesActivePods(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 0},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 100},
		},
	}

	result, err := hc.getDesiredReplica(metrics, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, result)
}

func TestGetDesiredReplicaNoRateAvailable(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 100},
		},
	}

	_, err := hc.getDesiredReplica(metrics, 4)
	assert.Error(t, err)
}

func TestGetDesiredReplicaNoActivePods(t *testing.T) {
	targetProcessingSeconds := uint32(5)
	monoVertex := &v1alpha1.MonoVertex{
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{TargetProcessingSeconds: &targetProcessingSeconds},
		},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 100},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 500},
		},
	}

	_, err := hc.getDesiredReplica(metrics, 0)
	assert.Error(t, err)
}

func TestGetDesiredReplicaOverflowClamped(t *testing.T) {
	targetProcessingSeconds := uint32(1)
	monoVertex := &v1alpha1.MonoVertex{
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{TargetProcessingSeconds: &targetProcessingSeconds},
		},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 0.001},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 1_000_000_000_000},
		},
	}

	result, err := hc.getDesiredReplica(metrics, 10)
	require.NoError(t, err)
	assert.Equal(t, int(math.MaxInt32), result)
}

func TestGetDesiredReplicaPendingNotAvailable(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 100},
		},
	}

	_, err := hc.getDesiredReplica(metrics, 1)
	assert.Error(t, err)
}

func TestGetMonoVertexDataCriticalityHealthy(t *testing.T) {
	targetProcessingSeconds := uint32(5)
	monoVertex := &v1alpha1.MonoVertex{
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{TargetProcessingSeconds: &targetProcessingSeconds},
		},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 100},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 500},
		},
	}

	state, err := hc.getMonoVertexDataCriticality(context.Background(), metrics, 4)
	require.NoError(t, err)
	assert.Equal(t, v1alpha1.MonoVertexStatusHealthy, state.State)
}

func TestGetMonoVertexDataCriticalityWarning(t *testing.T) {
	targetProcessingSeconds := uint32(5)
	monoVertex := &v1alpha1.MonoVertex{
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{TargetProcessingSeconds: &targetProcessingSeconds},
		},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{
			"default": {Value: 100},
		},
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 1000},
		},
	}

	state, err := hc.getMonoVertexDataCriticality(context.Background(), metrics, 4)
	require.NoError(t, err)
	assert.Equal(t, v1alpha1.MonoVertexStatusWarning, state.State)
}

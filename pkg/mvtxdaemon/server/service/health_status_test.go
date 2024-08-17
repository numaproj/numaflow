package service

import (
	"testing"

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
		Status: v1alpha1.MonoVertexStatus{Replicas: 4},
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

	expected := int(4)
	result, err := hc.getDesiredReplica(metrics)
	if err != nil {
		t.Fatal(err)
	}
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}

func TestGetDesiredReplicaNoRateAvailable(t *testing.T) {
	monoVertex := &v1alpha1.MonoVertex{
		Status: v1alpha1.MonoVertexStatus{Replicas: 4},
	}
	hc := NewHealthChecker(monoVertex)

	metrics := &mvtxdaemon.MonoVertexMetrics{
		MonoVertex: "vertex",
		Pendings: map[string]*wrapperspb.Int64Value{
			"default": {Value: 100},
		},
	}

	_, err := hc.getDesiredReplica(metrics)
	if err == nil {
		t.Errorf("Expected error for no rate information, got nil")
	}
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

	_, err := hc.getDesiredReplica(metrics)
	if err == nil {
		t.Errorf("Expected error for no pending information, got nil")
	}
}

//
//func TestGetMonoVertexDataCriticality(t *testing.T) {
//	maxReplicas := int32(5)
//	monoVertex := &v1alpha1.MonoVertex{
//		Spec: v1alpha1.MonoVertexSpec{
//			Scale: v1alpha1.Scale{
//				Max: &maxReplicas,
//			},
//		},
//	}
//	hc := NewHealthChecker(monoVertex)
//
//	tests := []struct {
//		name            string
//		metrics         *mvtxdaemon.MonoVertexMetrics
//		currentReplicas int
//		desiredReplicas int
//		expectedState   string
//	}{
//		{
//			name: "Current equal max, desired more than max (Critical)",
//			metrics: &mvtxdaemon.MonoVertexMetrics{
//				MonoVertex: "vertex",
//			},
//			currentReplicas: 5,
//			desiredReplicas: 6,
//			expectedState:   v1alpha1.MonoVertexStatusCritical,
//		},
//		{
//			name: "Current less than max, desired more than max (Healthy)",
//			metrics: &mvtxdaemon.MonoVertexMetrics{
//				MonoVertex: "vertex",
//			},
//			currentReplicas: 4,
//			desiredReplicas: 6,
//			expectedState:   v1alpha1.MonoVertexStatusHealthy,
//		},
//	}
//
//	for _, test := range tests {
//		t.Run(test.name, func(t *testing.T) {
//			hc.monoVertex.Status.Replicas = uint32(test.currentReplicas)
//			// Mock getDesiredReplica to return desiredReplicas directly
//			hc.getDesiredReplica = func(*mvtxdaemon.MonoVertexMetrics) (int, error) {
//				return test.desiredReplicas, nil
//			}
//
//			vertexState, err := hc.getMonoVertexDataCriticality(context.Background(), test.metrics)
//			if err != nil {
//				t.Fatal(err)
//			}
//			if vertexState.State != test.expectedState {
//				t.Errorf("Expected state %s, got %s", test.expectedState, vertexState.State)
//			}
//		})
//	}
//}

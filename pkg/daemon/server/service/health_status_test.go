package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

func TestNewDataHealthResponse(t *testing.T) {
	tests := []struct {
		name           string
		status         string
		message        string
		code           string
		expectedStatus string
		expectedMsg    string
		expectedCode   string
	}{
		{
			name:           "Healthy response",
			status:         "healthy",
			message:        "All systems operational",
			code:           "200",
			expectedStatus: "healthy",
			expectedMsg:    "All systems operational",
			expectedCode:   "200",
		},
		{
			name:           "Unhealthy response",
			status:         "unhealthy",
			message:        "Service degradation detected",
			code:           "503",
			expectedStatus: "unhealthy",
			expectedMsg:    "Service degradation detected",
			expectedCode:   "503",
		},
		{
			name:           "Empty response",
			status:         "",
			message:        "",
			code:           "",
			expectedStatus: "",
			expectedMsg:    "",
			expectedCode:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := newDataHealthResponse(tt.status, tt.message, tt.code)
			assert.NotNil(t, response)
			assert.Equal(t, tt.expectedStatus, response.Status)
			assert.Equal(t, tt.expectedMsg, response.Message)
			assert.Equal(t, tt.expectedCode, response.Code)
		})
	}
}

func TestNewHealthChecker(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *v1alpha1.Pipeline
		isbSvc   isbsvc.ISBService
	}{
		{
			name:     "With nil pipeline and nil ISBService",
			pipeline: nil,
			isbSvc:   nil,
		},
		{
			name:     "With non-nil pipeline and nil ISBService",
			pipeline: &v1alpha1.Pipeline{},
			isbSvc:   nil,
		},
		{
			name:     "With nil pipeline and non-nil ISBService",
			pipeline: nil,
			isbSvc:   &mockISBService{},
		},
		{
			name:     "With non-nil pipeline and non-nil ISBService",
			pipeline: &v1alpha1.Pipeline{},
			isbSvc:   &mockISBService{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := NewHealthChecker(tt.pipeline, tt.isbSvc)
			assert.NotNil(t, hc)
			assert.Equal(t, defaultDataHealthResponse, hc.currentDataStatus)
			assert.Equal(t, tt.isbSvc, hc.isbSvcClient)
			assert.Equal(t, tt.pipeline, hc.pipeline)
			assert.NotNil(t, hc.timelineData)
			assert.Empty(t, hc.timelineData)
			assert.NotNil(t, hc.statusLock)
		})
	}
}

func TestHealthThresholds(t *testing.T) {

	hc := NewHealthChecker(&v1alpha1.Pipeline{}, &mockISBService{})
	a, _ := hc.GetThresholds()
	assert.Equal(t, a, float64(72))

	forty := uint32(40)
	eighty := uint32(80)

	hc.pipeline.Spec.Limits = &v1alpha1.PipelineLimits{BufferUsageLimit: &forty}
	hc.udpateThresholds(uint32(*hc.pipeline.Spec.Limits.BufferUsageLimit))
	c, _ := hc.GetThresholds()
	assert.Equal(t, c, float64(36))

	hc.pipeline.Spec.Limits = &v1alpha1.PipelineLimits{BufferUsageLimit: &eighty}
	hc.udpateThresholds(uint32(*hc.pipeline.Spec.Limits.BufferUsageLimit))

}

type mockISBService struct {
	isbsvc.ISBService
}

func TestNewVertexState(t *testing.T) {
	tests := []struct {
		name          string
		vertexName    string
		vertexState   string
		expectedName  string
		expectedState string
	}{
		{
			name:          "Normal vertex state",
			vertexName:    "vertex1",
			vertexState:   "running",
			expectedName:  "vertex1",
			expectedState: "running",
		},
		{
			name:          "Empty vertex name",
			vertexName:    "",
			vertexState:   "pending",
			expectedName:  "",
			expectedState: "pending",
		},
		{
			name:          "Empty vertex state",
			vertexName:    "vertex2",
			vertexState:   "",
			expectedName:  "vertex2",
			expectedState: "",
		},
		{
			name:          "Special characters in vertex name",
			vertexName:    "vertex-3_@#$",
			vertexState:   "completed",
			expectedName:  "vertex-3_@#$",
			expectedState: "completed",
		},
		{
			name:          "Long vertex name and state",
			vertexName:    "very_long_vertex_name_that_exceeds_normal_length",
			vertexState:   "very_long_state_description_that_exceeds_normal_length",
			expectedName:  "very_long_vertex_name_that_exceeds_normal_length",
			expectedState: "very_long_state_description_that_exceeds_normal_length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := newVertexState(tt.vertexName, tt.vertexState)
			assert.NotNil(t, result)
			assert.Equal(t, tt.expectedName, result.Name)
			assert.Equal(t, tt.expectedState, result.State)
		})
	}
}

func TestGetCurrentHealth(t *testing.T) {
	tests := []struct {
		name           string
		initialStatus  *dataHealthResponse
		expectedStatus *dataHealthResponse
	}{
		{
			name: "Default health status",
			initialStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
			expectedStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
		},
		{
			name: "Custom health status",
			initialStatus: &dataHealthResponse{
				Status:  "degraded",
				Message: "Partial system outage",
				Code:    "503",
			},
			expectedStatus: &dataHealthResponse{
				Status:  "degraded",
				Message: "Partial system outage",
				Code:    "503",
			},
		},
		{
			name:           "Nil health status",
			initialStatus:  nil,
			expectedStatus: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := &HealthChecker{
				currentDataStatus: tt.initialStatus,
				statusLock:        &sync.RWMutex{},
			}

			result := hc.getCurrentHealth()
			assert.Equal(t, tt.expectedStatus, result)
		})
	}
}

func TestGetCurrentHealthConcurrency(t *testing.T) {
	hc := &HealthChecker{
		currentDataStatus: &dataHealthResponse{
			Status:  "healthy",
			Message: "All systems operational",
			Code:    "200",
		},
		statusLock: &sync.RWMutex{},
	}

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			result := hc.getCurrentHealth()
			assert.NotNil(t, result)
			assert.Equal(t, "healthy", result.Status)
			assert.Equal(t, "All systems operational", result.Message)
			assert.Equal(t, "200", result.Code)
		}()
	}

	wg.Wait()
}

func TestGetCurrentHealthWithUpdates(t *testing.T) {
	hc := &HealthChecker{
		currentDataStatus: &dataHealthResponse{
			Status:  "healthy",
			Message: "All systems operational",
			Code:    "200",
		},
		statusLock: &sync.RWMutex{},
	}

	// Initial check
	result := hc.getCurrentHealth()
	assert.Equal(t, "healthy", result.Status)

	// Update health status
	hc.statusLock.Lock()
	hc.currentDataStatus = &dataHealthResponse{
		Status:  "unhealthy",
		Message: "System outage",
		Code:    "503",
	}
	hc.statusLock.Unlock()

	// Check updated status
	result = hc.getCurrentHealth()
	assert.Equal(t, "unhealthy", result.Status)
	assert.Equal(t, "System outage", result.Message)
	assert.Equal(t, "503", result.Code)
}

func TestSetCurrentHealth(t *testing.T) {
	tests := []struct {
		name           string
		initialStatus  *dataHealthResponse
		newStatus      *dataHealthResponse
		expectedStatus *dataHealthResponse
	}{
		{
			name: "Update from healthy to unhealthy",
			initialStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
			newStatus: &dataHealthResponse{
				Status:  "unhealthy",
				Message: "System failure",
				Code:    "500",
			},
			expectedStatus: &dataHealthResponse{
				Status:  "unhealthy",
				Message: "System failure",
				Code:    "500",
			},
		},
		{
			name: "Update from unhealthy to healthy",
			initialStatus: &dataHealthResponse{
				Status:  "unhealthy",
				Message: "System failure",
				Code:    "500",
			},
			newStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
			expectedStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
		},
		{
			name:          "Update from nil to valid status",
			initialStatus: nil,
			newStatus: &dataHealthResponse{
				Status:  "degraded",
				Message: "Partial system outage",
				Code:    "503",
			},
			expectedStatus: &dataHealthResponse{
				Status:  "degraded",
				Message: "Partial system outage",
				Code:    "503",
			},
		},
		{
			name: "Update to nil status",
			initialStatus: &dataHealthResponse{
				Status:  "healthy",
				Message: "All systems operational",
				Code:    "200",
			},
			newStatus:      nil,
			expectedStatus: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := &HealthChecker{
				currentDataStatus: tt.initialStatus,
				statusLock:        &sync.RWMutex{},
			}

			hc.setCurrentHealth(tt.newStatus)

			assert.Equal(t, tt.expectedStatus, hc.currentDataStatus)
		})
	}
}

func TestSetCurrentHealthConcurrency(t *testing.T) {
	hc := &HealthChecker{
		currentDataStatus: &dataHealthResponse{
			Status:  "healthy",
			Message: "All systems operational",
			Code:    "200",
		},
		statusLock: &sync.RWMutex{},
	}

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			newStatus := &dataHealthResponse{
				Status:  fmt.Sprintf("status_%d", i),
				Message: fmt.Sprintf("message_%d", i),
				Code:    fmt.Sprintf("%d", i),
			}
			hc.setCurrentHealth(newStatus)
		}(i)
	}

	wg.Wait()

	finalStatus := hc.getCurrentHealth()
	assert.NotNil(t, finalStatus)
	assert.Contains(t, finalStatus.Status, "status_")
	assert.Contains(t, finalStatus.Message, "message_")
	assert.NotEqual(t, "", finalStatus.Code)
}

func TestSetCurrentHealthWithGetCurrentHealth(t *testing.T) {
	hc := &HealthChecker{
		currentDataStatus: &dataHealthResponse{
			Status:  "healthy",
			Message: "All systems operational",
			Code:    "200",
		},
		statusLock: &sync.RWMutex{},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			newStatus := &dataHealthResponse{
				Status:  fmt.Sprintf("status_%d", i),
				Message: fmt.Sprintf("message_%d", i),
				Code:    fmt.Sprintf("%d", i),
			}
			hc.setCurrentHealth(newStatus)
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			status := hc.getCurrentHealth()
			assert.NotNil(t, status)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	finalStatus := hc.getCurrentHealth()
	assert.NotNil(t, finalStatus)
	assert.Contains(t, finalStatus.Status, "status_")
	assert.Contains(t, finalStatus.Message, "message_")
	assert.NotEqual(t, "", finalStatus.Code)
}

func TestUpdateUsageTimeline(t *testing.T) {
	tests := []struct {
		name       string
		bufferList []*daemon.BufferInfo
		expected   map[string]int
	}{
		{
			name: "Single buffer update",
			bufferList: []*daemon.BufferInfo{
				{
					BufferName:  "buffer1",
					BufferUsage: wrapperspb.Double(0.5),
				},
			},
			expected: map[string]int{
				"buffer1": 1,
			},
		},
		{
			name: "Multiple buffer update",
			bufferList: []*daemon.BufferInfo{
				{
					BufferName:  "buffer1",
					BufferUsage: wrapperspb.Double(0.3),
				},
				{
					BufferName:  "buffer2",
					BufferUsage: wrapperspb.Double(0.7),
				},
			},
			expected: map[string]int{
				"buffer1": 1,
				"buffer2": 1,
			},
		},
		{
			name:       "Empty buffer list",
			bufferList: []*daemon.BufferInfo{},
			expected:   map[string]int{},
		},
		{
			name: "Buffer with zero usage",
			bufferList: []*daemon.BufferInfo{
				{
					BufferName:  "buffer1",
					BufferUsage: wrapperspb.Double(0),
				},
			},
			expected: map[string]int{
				"buffer1": 1,
			},
		},
		{
			name: "Buffer with full usage",
			bufferList: []*daemon.BufferInfo{
				{
					BufferName:  "buffer1",
					BufferUsage: wrapperspb.Double(1),
				},
			},
			expected: map[string]int{
				"buffer1": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := &HealthChecker{
				timelineData: make(map[string]*sharedqueue.OverflowQueue[*timelineEntry]),
			}

			hc.updateUsageTimeline(tt.bufferList)

			for bufferName, expectedCount := range tt.expected {
				assert.NotNil(t, hc.timelineData[bufferName])
				assert.Equal(t, expectedCount, hc.timelineData[bufferName].Length())

				if expectedCount > 0 {
					lastEntry := hc.timelineData[bufferName].Items()[0]
					assert.NotZero(t, lastEntry.Time)
				}
			}
		})
	}
}

func TestUpdateAverageBufferUsage(t *testing.T) {
	tests := []struct {
		name     string
		timeline []*timelineEntry
		newEntry float64
		expected float64
	}{
		{
			name:     "Empty timeline",
			timeline: []*timelineEntry{},
			newEntry: 0.5,
			expected: 0.5,
		},
		{
			name: "Timeline with one entry",
			timeline: []*timelineEntry{
				{BufferUsage: 0.3, AverageBufferUsage: 0.3},
			},
			newEntry: 0.7,
			expected: 0.5,
		},
		{
			name: "Full timeline",
			timeline: []*timelineEntry{
				{BufferUsage: 0.1, AverageBufferUsage: 0.1},
				{BufferUsage: 0.2, AverageBufferUsage: 0.15},
				{BufferUsage: 0.3, AverageBufferUsage: 0.2},
				{BufferUsage: 0.4, AverageBufferUsage: 0.25},
				{BufferUsage: 0.5, AverageBufferUsage: 0.3},
			},
			newEntry: 0.6,
			expected: 0.35,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := updateAverageBufferUsage(tt.timeline, tt.newEntry)
			assert.InDelta(t, tt.expected, result, 0.001, "Expected average buffer usage to be %v, but got %v", tt.expected, result)
		})
	}
}

func TestAssignStateToBufferUsage(t *testing.T) {
	tests := []struct {
		name      string
		ewmaValue float64
		expected  string
	}{
		{
			name:      "Critical state",
			ewmaValue: v1alpha1.DefaultBufferUsageLimit * 95,
			expected:  criticalState,
		},
		{
			name:      "Warning state",
			ewmaValue: v1alpha1.DefaultBufferUsageLimit * 90 * 0.86,
			expected:  warningState,
		},
		{
			name:      "Healthy state",
			ewmaValue: v1alpha1.DefaultBufferUsageLimit * 30,
			expected:  healthyState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignStateToBufferUsage(tt.ewmaValue, 80)
			t.Log(tt.ewmaValue, result)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAssignStateToTimeline(t *testing.T) {
	n := v1alpha1.DefaultBufferUsageLimit * 100
	tests := []struct {
		name       string
		ewmaValues []float64
		lookBack   bool
		expected   string
	}{
		{
			name:       "Single healthy value",
			ewmaValues: []float64{n * 0.3},
			lookBack:   false,
			expected:   healthyState,
		},
		{
			name:       "Single warning value",
			ewmaValues: []float64{n * 0.85},
			lookBack:   false,
			expected:   warningState,
		},
		{
			name:       "Single critical value without lookback",
			ewmaValues: []float64{n * 0.95},
			lookBack:   false,
			expected:   criticalState,
		},
		{
			name:       "Single critical value with lookback",
			ewmaValues: []float64{n * 0.95},
			lookBack:   true,
			expected:   warningState,
		},
		{
			name:       "Multiple values ending with critical, no lookback",
			ewmaValues: []float64{n * 0.3, n * 0.7, n * 0.92},
			lookBack:   false,
			expected:   criticalState,
		},
		{
			name:       "Multiple values ending with critical, with lookback, insufficient critical count",
			ewmaValues: []float64{n * 0.3, n * 0.7, n * 0.95, n * 0.95},
			lookBack:   true,
			expected:   warningState,
		},
		{
			name:       "Multiple values ending with critical, with lookback, sufficient critical count",
			ewmaValues: []float64{n * 0.95, n * 0.95, n * 0.95, n * 0.95, n * 0.95},
			lookBack:   true,
			expected:   criticalState,
		},
		{
			name:       "Values fluctuating between warning and critical",
			ewmaValues: []float64{n * 0.85, n * 0.95, n * 0.85, n * 0.95, n * 0.85},
			lookBack:   true,
			expected:   warningState,
		},
		{
			name:       "Values increasing from healthy to critical",
			ewmaValues: []float64{n * 0.3, n * 0.5, n * 0.7, n * 0.9, n * 0.96},
			lookBack:   true,
			expected:   warningState,
		},
		{
			name:       "Values decreasing from critical to healthy",
			ewmaValues: []float64{n * 0.96, n * 0.9, n * 0.7, n * 0.5, n * 0.3},
			lookBack:   true,
			expected:   healthyState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignStateToTimeline(tt.ewmaValues, tt.lookBack, 80)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// func Scalalbility_error_test(t *testing.T) {
// 	pipelineName := "simple-pipeline"
// 	namespace := "numaflow-system"
// 	edges := []v1alpha1.Edge{
// 		{
// 			From: "in",
// 			To:   "cat",
// 		},
// 		{
// 			From: "cat",
// 			To:   "out",
// 		},
// 	}
// 	pipeline := &v1alpha1.Pipeline{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      pipelineName,
// 			Namespace: namespace,
// 		},
// 		Spec: v1alpha1.PipelineSpec{
// 			Vertices: []v1alpha1.AbstractVertex{
// 				{Name: "in", Source: &v1alpha1.Source{}},
// 				{Name: "cat", UDF: &v1alpha1.UDF{}},
// 				{Name: "out", Sink: &v1alpha1.Sink{}},
// 			},
// 			Edges: edges,
// 		},
// 	}

// 	for v := range pipeline.Spec.Vertices {
// 		pipeline.Spec.Vertices[v].Scale = v1alpha1.Scale{Disabled: true}
// 	}

// 	pipeline.

// }

func TestConvertVertexStateToPipelineState(t *testing.T) {
	tests := []struct {
		name         string
		vertexStates []*vertexState
		expected     *dataHealthResponse
	}{
		{
			name: "All vertices healthy",
			vertexStates: []*vertexState{
				{Name: "vertex1", State: v1alpha1.PipelineStatusHealthy},
				{Name: "vertex2", State: v1alpha1.PipelineStatusHealthy},
			},
			expected: newDataHealthResponse(v1alpha1.PipelineStatusHealthy, "Pipeline data flow is healthy", "D1"),
		},
		{
			name: "One vertex warning",
			vertexStates: []*vertexState{
				{Name: "vertex1", State: v1alpha1.PipelineStatusHealthy},
				{Name: "vertex2", State: v1alpha1.PipelineStatusWarning},
			},
			expected: newDataHealthResponse(v1alpha1.PipelineStatusWarning, "Pipeline data flow is in a warning state for vertex2", "D2"),
		},
		{
			name: "One vertex critical",
			vertexStates: []*vertexState{
				{Name: "vertex1", State: v1alpha1.PipelineStatusHealthy},
				{Name: "vertex2", State: v1alpha1.PipelineStatusCritical},
			},
			expected: newDataHealthResponse(v1alpha1.PipelineStatusCritical, "Pipeline data flow is in a critical state for vertex2", "D3"),
		},
		{
			name: "One vertex unknown",
			vertexStates: []*vertexState{
				{Name: "vertex1", State: v1alpha1.PipelineStatusHealthy},
				{Name: "vertex2", State: v1alpha1.PipelineStatusUnknown},
			},
			expected: newDataHealthResponse(v1alpha1.PipelineStatusUnknown, "Pipeline data flow is in an unknown state due to vertex2", "D4"),
		},
		{
			name:         "Empty vertex state list",
			vertexStates: []*vertexState{},
			expected:     newDataHealthResponse(v1alpha1.PipelineStatusHealthy, "Pipeline data flow is healthy", "D1"),
		},
		{
			name:         "Nil vertex state list",
			vertexStates: nil,
			expected:     newDataHealthResponse(v1alpha1.PipelineStatusHealthy, "Pipeline data flow is healthy", "D1"),
		},
		{
			name: "Multiple vertices with same highest state",
			vertexStates: []*vertexState{
				{Name: "vertex1", State: v1alpha1.PipelineStatusHealthy},
				{Name: "vertex2", State: v1alpha1.PipelineStatusWarning},
				{Name: "vertex3", State: v1alpha1.PipelineStatusWarning},
			},
			expected: newDataHealthResponse(v1alpha1.PipelineStatusWarning, "Pipeline data flow is in a warning state for vertex2", "D2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertVertexStateToPipelineState(tt.vertexStates)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateDataHealthResponse(t *testing.T) {
	tests := []struct {
		name           string
		state          string
		vertex         string
		expectedStatus string
		expectedMsg    string
		expectedCode   string
	}{
		{
			name:           "Healthy state",
			state:          v1alpha1.PipelineStatusHealthy,
			vertex:         "vertex1",
			expectedStatus: v1alpha1.PipelineStatusHealthy,
			expectedMsg:    "Pipeline data flow is healthy",
			expectedCode:   "D1",
		},
		{
			name:           "Warning state",
			state:          v1alpha1.PipelineStatusWarning,
			vertex:         "vertex2",
			expectedStatus: v1alpha1.PipelineStatusWarning,
			expectedMsg:    "Pipeline data flow is in a warning state for vertex2",
			expectedCode:   "D2",
		},
		{
			name:           "Critical state",
			state:          v1alpha1.PipelineStatusCritical,
			vertex:         "vertex3",
			expectedStatus: v1alpha1.PipelineStatusCritical,
			expectedMsg:    "Pipeline data flow is in a critical state for vertex3",
			expectedCode:   "D3",
		},
		{
			name:           "Unknown state",
			state:          v1alpha1.PipelineStatusUnknown,
			vertex:         "vertex4",
			expectedStatus: v1alpha1.PipelineStatusUnknown,
			expectedMsg:    "Pipeline data flow is in an unknown state due to vertex4",
			expectedCode:   "D4",
		},
		{
			name:           "Invalid state",
			state:          "invalid",
			vertex:         "vertex5",
			expectedStatus: defaultDataHealthResponse.Status,
			expectedMsg:    defaultDataHealthResponse.Message,
			expectedCode:   defaultDataHealthResponse.Code,
		},
		{
			name:           "Empty state and vertex",
			state:          "",
			vertex:         "",
			expectedStatus: defaultDataHealthResponse.Status,
			expectedMsg:    defaultDataHealthResponse.Message,
			expectedCode:   defaultDataHealthResponse.Code,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateDataHealthResponse(tt.state, tt.vertex)
			assert.NotNil(t, result)
			assert.Equal(t, tt.expectedStatus, result.Status)
			assert.Equal(t, tt.expectedMsg, result.Message)
			assert.Equal(t, tt.expectedCode, result.Code)
		})
	}
}

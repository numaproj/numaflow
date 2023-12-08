package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/VividCortex/ewma"
	"golang.org/x/net/context"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	// HealthSlidingWindow is the total time window to compute the health of a vertex
	HealthSlidingWindow = 5 * time.Minute

	// HealthTimeStep is the frequency at which the health of a vertex is computed
	HealthTimeStep = 10 * time.Second

	// HealthWindowSize is the number of timeline entries to keep for a given vertex
	// This is used to compute the health of a vertex
	// Ensure that HealthSlidingWindow / HealthTimeStep is an integer
	HealthWindowSize = HealthSlidingWindow / HealthTimeStep

	// CriticalWindowTime is the number of entries to look back to assign a critical state to a vertex
	CriticalWindowTime = 1 * time.Minute

	// CriticalWindowSize is the number of entries to look back to assign a critical state to a vertex
	// This is used to avoid false positives
	CriticalWindowSize = int(CriticalWindowTime / HealthTimeStep)

	// CriticalLookBackCount is the number of times the state must be critical in the last CriticalWindowSize entries
	// This is used to avoid false positives
	CriticalLookBackCount = 3
)

// HealthThresholds are the thresholds used to compute the health of a vertex
const (
	// CriticalThreshold is the threshold above which the health of a vertex is critical
	CriticalThreshold = 95
	// WarningThreshold is the threshold above which the health of a vertex is warning
	WarningThreshold = 80
)

// Dataflow states
const (
	// CriticalState is the state of a vertex when its health is critical
	CriticalState = "critical"
	// WarningState is the state of a vertex when its health is warning
	WarningState = "warning"
	// HealthyState is the state of a vertex when its health is healthy
	HealthyState = "healthy"
)

const (
	// EnableCriticalLookBack is used to enable the look back for critical state
	EnableCriticalLookBack  = true
	DisableCriticalLookBack = false
)

// TimelineEntry is used to store the buffer usage timeline for a given vertex
type TimelineEntry struct {
	// The time at which the entry is recorded
	Time int64 `json:"time"`
	// The buffer usage of the pipeline at the time
	BufferUsage float64 `json:"bufferUsage"`
	// The rolling average buffer usage of the pipeline over the last HEALTH_WINDOW_SIZE seconds
	AverageBufferUsage float64 `json:"averageBufferUsage"`
}

// DataHealthResponse is the response returned by the data health check API
type DataHealthResponse struct {
	// Status is the overall data status of the pipeline
	Status string `json:"status"`
	// Message is the error message if any
	Message string `json:"message"`
	// Code is the status code for the data health
	Code string `json:"code"`
}

// NewDataHealthResponse is used to create a new DataHealthResponse object
func NewDataHealthResponse(status string, message string, code string) *DataHealthResponse {
	return &DataHealthResponse{
		Status:  status,
		Message: message,
		Code:    code,
	}
}

// DefaultDataHealthResponse is the default response returned by the data health check API
var DefaultDataHealthResponse = NewDataHealthResponse(PipelineStatusOK,
	"Pipeline dataflow is healthy",
	"D1")

var (
	currentPipelineStatus = DefaultDataHealthResponse
	pipeStatusLock        = &sync.RWMutex{}
	log                   = logging.FromContext(context.Background())
)

// GetCurrentPipelineHealth returns the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func GetCurrentPipelineHealth() *DataHealthResponse {
	// Lock the statusLock to ensure thread safety.
	pipeStatusLock.RLock()
	defer pipeStatusLock.RUnlock()
	// Return the health status.
	return currentPipelineStatus
}

// SetCurrentPipelineHealth sets the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func SetCurrentPipelineHealth(status *DataHealthResponse) {
	// Lock the statusLock to ensure thread safety.
	pipeStatusLock.Lock()
	defer pipeStatusLock.Unlock()
	// Set the health status.
	currentPipelineStatus = status
}

// HealthChecker is the struct type for health checker.
type HealthChecker struct {
	// Add a field for the health status.
	currentDataStatus     *DataHealthResponse
	pipelineMetadataQuery *pipelineMetadataQuery
	pipeline              *v1alpha1.Pipeline
	timelineData          map[string][]*TimelineEntry
	statusLock            *sync.RWMutex
}

// NewHealthChecker creates a new object HealthChecker struct type.
func NewHealthChecker(pipeline *v1alpha1.Pipeline, pipelineMetadataQuery *pipelineMetadataQuery) *HealthChecker {
	// Return a new HealthChecker struct instance.
	return &HealthChecker{
		currentDataStatus:     DefaultDataHealthResponse,
		pipelineMetadataQuery: pipelineMetadataQuery,
		pipeline:              pipeline,
		timelineData:          make(map[string][]*TimelineEntry),
		statusLock:            &sync.RWMutex{},
	}
}

// VertexState is a struct which contains the name and  state of a vertex
type VertexState struct {
	// Name is the name of the vertex
	Name string `json:"name"`
	// State is the state of the vertex
	State string `json:"state"`
}

// NewVertexState is used to create a new VertexState object
func NewVertexState(name string, state string) *VertexState {
	return &VertexState{
		Name:  name,
		State: state,
	}
}

// GetCurrentHealth returns the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) GetCurrentHealth() *DataHealthResponse {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.RLock()
	defer hc.statusLock.RUnlock()
	// Return the health status.
	return hc.currentDataStatus
}

// SetCurrentHealth sets the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) SetCurrentHealth(status *DataHealthResponse) {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.Lock()
	defer hc.statusLock.Unlock()
	// Set the health status.
	hc.currentDataStatus = status
	SetCurrentPipelineHealth(status)
}

// StartHealthCheck starts the health check.
func (hc *HealthChecker) StartHealthCheck() {
	ctx := context.Background()
	// Goroutine to listen for ticks
	// At every tick, check and update the health status of the pipeline.
	go func() {
		// Create a ticker with the interval of HealthCheckInterval.
		ticker := time.NewTicker(HealthTimeStep)
		defer ticker.Stop()
		for {
			select {
			// If the ticker ticks, check and update the health status of the pipeline.
			case <-ticker.C:
				// Get the current health status of the pipeline.
				criticality, err := hc.getPipelineDataCriticality()
				if err != nil {
					return
				}
				// convert the vertex state to pipeline state
				pipelineState := convertVertexStateToPipelineState(criticality)
				// update the current health status of the pipeline
				hc.SetCurrentHealth(pipelineState)

			// If the context is done, return.
			case <-ctx.Done():
				return
			}
		}
	}()
}

// getPipelineDataCriticality is used to provide the data criticality of the pipeline
// They can be of the following types:
// 1. Ok: The pipeline is working as expected
// 2. Warning: The pipeline is working but there is a lag in the data movement
// 3. Critical: The pipeline is not working as expected
// We need to check the following things to determine the data criticality of the pipeline:
// 1. The buffer usage of each buffer in the pipeline
//
// Based on this information, we first get the current buffer usage of each buffer in the pipeline at that instant
// and populate the timeline data for each buffer.
// Then for a given buffer, we calculate the average buffer usage over
// the last HEALTH_WINDOW_SIZE seconds.
// If this average is greater than the critical threshold, we return the critical status
// If this average is greater than the warning threshold, we return the warning status
// If this average is less than the warning threshold, we return the ok status
func (hc *HealthChecker) getPipelineDataCriticality() ([]*VertexState, error) {
	ctx := context.Background()
	pipelineName := hc.pipeline.GetName()

	// Create a new buffer request object
	req := &daemon.ListBuffersRequest{Pipeline: &pipelineName}

	// Fetch the buffer information for the pipeline
	buffers, err := hc.pipelineMetadataQuery.ListBuffers(ctx, req)
	if err != nil {
		return nil, err
	}
	// update the usage timeline for all the ISBs used in the pipeline
	hc.updateUsageTimeline(buffers.Buffers)

	var vertexState []*VertexState

	// iterate over the timeline data for each buffer and calculate the exponential weighted mean average
	// for the last HEALTH_WINDOW_SIZE buffer usage entries
	for bufferName, timeline := range hc.timelineData {
		// Extract the buffer usage of the timeline
		var bufferUsage []float64
		for _, entry := range timeline {
			bufferUsage = append(bufferUsage, entry.BufferUsage)
		}
		// calculate the average buffer usage over the last HEALTH_WINDOW_SIZE seconds
		log.Info("DEBUGSID bufferusage", bufferName, bufferUsage)
		ewmaBufferUsage := calculateEWMAUsage(bufferUsage)
		log.Info("DEBUGSID ewmavalue", bufferName, ewmaBufferUsage)
		// assign the state to the vertex based on the average buffer usage
		// Look back is disabled for the critical state
		currentState := assignStateToTimeline(ewmaBufferUsage, EnableCriticalLookBack)
		// create a new vertex state object
		currentVertexState := NewVertexState(bufferName, currentState)
		// add the vertex state to the list of vertex states
		vertexState = append(vertexState, currentVertexState)
	}
	return vertexState, nil
}

// updateUsageTimeline is used to update the usage timeline for a given buffer list
// This iterates over all the buffers in the buffer list and updates the usage timeline for each buffer
// The timeline data is represented as a map of buffer name to a list of TimelineEntry
// Example:
//
//	{
//		"bufferName": [
//			{
//				"time": 1234567890,
//				"bufferUsage": 0.5,
//				"averageBufferUsage": 0.5
//			},
func (hc *HealthChecker) updateUsageTimeline(bufferList []*daemon.BufferInfo) {
	// iterate over the buffer list and update the usage timeline for each buffer
	for _, buffer := range bufferList {
		// get the buffer name and the current timestamp
		bufferName := buffer.GetBufferName()
		timestamp := time.Now().Unix()

		// if the buffer name is not present in the timeline data, add it
		if _, ok := hc.timelineData[bufferName]; !ok {
			hc.timelineData[bufferName] = make([]*TimelineEntry, 0)
		}
		// extract the current buffer usage and update the average buffer usage
		bufferUsage := buffer.GetBufferUsage() * 100
		newAverage := updateAverageBufferUsage(hc.timelineData[bufferName], bufferUsage)

		// add the new entry to the timeline
		hc.timelineData[bufferName] = append(hc.timelineData[bufferName], &TimelineEntry{
			Time:               timestamp,
			BufferUsage:        bufferUsage,
			AverageBufferUsage: newAverage,
		})
		log.Info("DEBUGSID", bufferName, bufferUsage, newAverage)

		// remove first entry if the size of the timeline is greater than HEALTH_WINDOW_SIZE
		if len(hc.timelineData[bufferName]) > int(HealthWindowSize) {
			hc.timelineData[bufferName] = hc.timelineData[bufferName][1:]
		}

	}
}

// updateAverageBufferUsage computes the average buffer usage of a vertex over the last HEALTH_WINDOW_SIZE seconds
// This is a rolling average, so the oldest entry is removed from the timeline and the new entry is added
// The average buffer usage is computed as the average of the buffer usage of the last HEALTH_WINDOW_SIZE entries
// If the timeline is empty, the average buffer usage is set to the buffer usage of the new entry
// If the timeline is not empty, the average buffer usage is computed as follows:
//
//	averageBufferUsage = lastAverage*HEALTH_WINDOW_SIZE + (newUsageEntry - oldestEntry) / HEALTH_WINDOW_SIZE
//	where oldestEntry is the oldest entry in the timeline
//	and lastAverage is the average buffer usage of the last entry in the timeline
//
// If the timeline size is less than HEALTH_WINDOW_SIZE, the average buffer usage is computed as follows:
//
//	averageBufferUsage = lastAverage * timelineSize + (newUsageEntry) / timelineSize + 1
//	where lastAverage is the average buffer usage of the last entry in the timeline
//	and timelineSize is the size of the timeline
func updateAverageBufferUsage(timeline []*TimelineEntry, newEntry float64) float64 {
	// If the timeline is empty, return the buffer usage of the new entry
	if len(timeline) == 0 {
		return newEntry
	}
	// Compute the current average buffer usage of the last entry
	lastAverage := timeline[len(timeline)-1].AverageBufferUsage
	var newAverage float64
	// If the timeline is full
	if len(timeline) == int(HealthWindowSize) {
		newAverage = (lastAverage*float64(HealthWindowSize) + newEntry - timeline[0].BufferUsage) / float64(HealthWindowSize)
	} else if len(timeline) < int(HealthWindowSize) {
		// If the timeline is not full
		newAverage = (lastAverage*float64(len(timeline)) + newEntry) / float64(len(timeline)+1)
	}
	return newAverage
}

// calculateEWMAUsage computes the EWMA buffer usage of a vertex over the last HEALTH_WINDOW_SIZE seconds
// This is an exponentially weighted moving average, with the following parameters:
// alpha = 2 / (HEALTH_WINDOW_SIZE + 1)
// EWMA = alpha * newUsageEntry + (1 - alpha) * lastEWMA
// where lastEWMA is the EWMA buffer usage of the last entry in the timeline
func calculateEWMAUsage(bufferUsage []float64) []float64 {
	// Compute the current EWMA buffer usage of the timeline
	a := ewma.NewMovingAverage()
	var emwaValues []float64
	for _, f := range bufferUsage {
		a.Add(f)
		emwaValues = append(emwaValues, a.Value())
	}
	return emwaValues
}

// assignStateToBufferUsage assigns a state to the buffer usage of a vertex
// The state is assigned as follows:
// - if the buffer usage is above CRITICAL_THRESHOLD, the state is set to CRITICAL
// - if the buffer usage is above WARNING_THRESHOLD, the state is set to WARNING
// - otherwise, the state is set to HEALTHY
func assignStateToBufferUsage(ewmaValue float64) string {
	// Assign the state to the buffer usage
	var state string
	if ewmaValue > CriticalThreshold {
		state = CriticalState
	} else if ewmaValue > WarningThreshold {
		state = WarningState
	} else {
		state = HealthyState
	}
	return state
}

// assignStateToTimeline assigns a state to the timeline of a vertex
// We also have a look back for critical state to avoid false positives
// In this case, we check if the state is CRITICAL at least LOOK_BACK_COUNT times in the last CRITICAL_WINDOW_SIZE entries
// If the state is CRITICAL at least LOOK_BACK_COUNT times in the last CRITICAL_WINDOW_SIZE entries
// Set the state to CRITICAL
func assignStateToTimeline(ewmaValues []float64, lookBack bool) string {
	// Extract the last entry of the timeline
	ewmaUsage := ewmaValues[len(ewmaValues)-1]

	// Assign the state to the buffer usage value
	state := assignStateToBufferUsage(ewmaUsage)

	// If the state is CRITICAL, and we have a look back, we need to check we have
	// LOOK_BACK_COUNT entries as CRITICAL
	if state == CriticalState && lookBack {
		// Extract the states of the timeline
		var states []string
		for _, entry := range ewmaValues {
			states = append(states, assignStateToBufferUsage(entry))
		}
		// Count the number of times the state is CRITICAL in the last CRITICAL_WINDOW_SIZE entries
		var criticalCount int
		for i := len(states) - 1; i >= 0; i-- {
			if states[i] == CriticalState {
				criticalCount++
			}
			if (len(states) - i) == CriticalWindowSize {
				break
			}
		}
		// If the state is CRITICAL at least LOOK_BACK_COUNT times in the last CRITICAL_WINDOW_SIZE entries
		// Set the state to CRITICAL, otherwise set the state to WARNING
		if criticalCount >= CriticalLookBackCount {
			state = CriticalState
		} else {
			state = WarningState
		}
	}
	return state
}

// convertVertexStateToPipelineState is used to convert the vertex state to pipeline state
// if any of the vertices are critical, the pipeline is critical
// if any of the vertices are warning, the pipeline is warning
// if any the vertices are unknown, the pipeline is unknown
// else the pipeline is ok
// Here we follow a precedence order of unknown > critical > warning  > ok
// Hence, whichever is the highest precedence state found in the vertex state, we return that
func convertVertexStateToPipelineState(vertexState []*VertexState) *DataHealthResponse {
	// create a map to store the precedence order of the states
	// assign a number to each state based on the precedence order

	// unknown > critical > warning > ok
	stateMap := map[string]int{
		PipelineStatusUnknown:  3,
		PipelineStatusCritical: 2,
		PipelineStatusWarning:  1,
		PipelineStatusOK:       0,
	}

	// initialize the max state to 0 (ie Healthy state)
	maxState := 0
	// initialize the max state vertex to empty string
	maxStateVtx := ""

	// iterate over the vertex state and assign a number to each state and update the current max state
	for _, state := range vertexState {
		if stateMap[state.State] > maxState {
			maxState = stateMap[state.State]
			maxStateVtx = state.Name
		}
	}

	// get the state and vertex corresponding to the max state
	for state, value := range stateMap {
		if value == maxState {
			return generateDataHealthResponse(state, maxStateVtx)
		}
	}

	// if we reach here, return unknown state
	return NewDataHealthResponse(PipelineStatusUnknown,
		"Pipeline dataflow is in an unknown state",
		"D4")
}

// generateDataHealthResponse is used to generate the data health response
// if the state is Healthy we return a message saying the pipeline is healthy, and the code corresponding to Healthy
// if the state is Warning we return a message saying the pipeline is warning due to the vertex,
// and the code corresponding to Warning, similar for Critical
// if the state is Unknown we return a message saying the pipeline is in an unknown state due to the vertex,
// and the code corresponding to Unknown
func generateDataHealthResponse(state string, vertex string) *DataHealthResponse {
	switch state {
	case PipelineStatusOK:
		return NewDataHealthResponse(
			PipelineStatusOK,
			"Pipeline dataflow is healthy",
			"D1")
	case PipelineStatusWarning:
		return NewDataHealthResponse(
			PipelineStatusWarning,
			fmt.Sprintf("Dataflow is in warning state for  %s", vertex),
			"D2")
	case PipelineStatusCritical:
		return NewDataHealthResponse(
			PipelineStatusCritical,
			fmt.Sprintf("Dataflow is in critical state for %s", vertex),
			"D3")
	case PipelineStatusUnknown:
		return NewDataHealthResponse(
			PipelineStatusUnknown,
			fmt.Sprintf("Pipeline dataflow is in an unknown state due to %s", vertex),
			"D4")
	default:
		return DefaultDataHealthResponse
	}
}

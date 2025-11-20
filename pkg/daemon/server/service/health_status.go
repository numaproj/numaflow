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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/ewma"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const (
	// healthSlidingWindow is the total time window to compute the health of a vertex
	healthSlidingWindow = 5 * time.Minute

	// healthTimeStep is the frequency at which the health of a vertex is computed
	healthTimeStep = 10 * time.Second

	// healthWindowSize is the number of timeline entries to keep for a given vertex
	// This is used to compute the health of a vertex
	// Ensure that healthSlidingWindow / healthTimeStep is an integer
	healthWindowSize = healthSlidingWindow / healthTimeStep

	// criticalWindowTime is the number of entries to look back to assign a critical state to a vertex
	// Critical state is assigned to a vertex if the weighted mean average buffer usage of
	// the vertex for a given time period is above a defined criticalBufferThreshold
	criticalWindowTime = 1 * time.Minute

	// criticalWindowSize is the number of entries to look back to assign a critical state to a vertex
	// This is used to avoid false positives
	criticalWindowSize = int(criticalWindowTime / healthTimeStep)

	// criticalLookBackCount is the number of times the state must be critical in the last criticalWindowSize entries
	// This is used to avoid false positives
	criticalLookBackCount = 3
)

// HealthThresholds are the thresholds used to compute the health of a vertex
const (
	// criticalBufferThreshold is the threshold above which the health of a vertex is critical
	criticalBufferThreshold = 95
	// warningBufferThreshold is the threshold above which the health of a vertex is warning
	warningBufferThreshold = 80
)

// Dataflow states
const (
	// criticalState is the state of a vertex when its health is critical
	criticalState = "critical"
	// warningState is the state of a vertex when its health is warning
	warningState = "warning"
	// healthyState is the state of a vertex when its health is healthy
	healthyState = "healthy"
)

const (
	// enableCriticalLookBack is used to enable the look back for critical state
	enableCriticalLookBack = true
)

// timelineEntry is used to store the buffer usage timeline for a given vertex
type timelineEntry struct {
	// The time at which the entry is recorded
	Time int64 `json:"time"`
	// The buffer usage of the pipeline at the time
	BufferUsage float64 `json:"bufferUsage"`
	// The rolling average buffer usage of the pipeline over the last HEALTH_WINDOW_SIZE seconds
	AverageBufferUsage float64 `json:"averageBufferUsage"`
}

// dataHealthResponse is the response returned by the data health check API
type dataHealthResponse struct {
	// Status is the overall data status of the pipeline
	Status string `json:"status"`
	// Message is the error message if any
	Message string `json:"message"`
	// Code is the status code for the data health
	Code string `json:"code"`
}

// newDataHealthResponse is used to create a new dataHealthResponse object
func newDataHealthResponse(status string, message string, code string) *dataHealthResponse {
	return &dataHealthResponse{
		Status:  status,
		Message: message,
		Code:    code,
	}
}

// defaultDataHealthResponse is the default response returned by the data health check API
var defaultDataHealthResponse = newDataHealthResponse(v1alpha1.PipelineStatusUnknown,
	"Pipeline data flow is in an unknown state",
	"D4")

// HealthChecker is the struct type for health checker.
type HealthChecker struct {
	// Add a field for the health status.
	currentDataStatus *dataHealthResponse
	isbSvcClient      isbsvc.ISBService
	pipeline          *v1alpha1.Pipeline
	timelineData      map[string]*sharedqueue.OverflowQueue[*timelineEntry]
	statusLock        *sync.RWMutex
}

// NewHealthChecker creates a new object HealthChecker struct type.
func NewHealthChecker(pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService) *HealthChecker {
	// Return a new HealthChecker struct instance.
	return &HealthChecker{
		currentDataStatus: defaultDataHealthResponse,
		isbSvcClient:      isbSvcClient,
		pipeline:          pipeline,
		timelineData:      make(map[string]*sharedqueue.OverflowQueue[*timelineEntry]),
		statusLock:        &sync.RWMutex{},
	}
}

// vertexState is a struct which contains the name and  state of a vertex
type vertexState struct {
	// Name is the name of the vertex
	Name string `json:"name"`
	// State is the state of the vertex
	State string `json:"state"`
}

// newVertexState is used to create a new vertexState object
func newVertexState(name string, state string) *vertexState {
	return &vertexState{
		Name:  name,
		State: state,
	}
}

// getCurrentHealth returns the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) getCurrentHealth() *dataHealthResponse {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.RLock()
	defer hc.statusLock.RUnlock()
	// Return the health status.
	return hc.currentDataStatus
}

// setCurrentHealth sets the current health status of the pipeline.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) setCurrentHealth(status *dataHealthResponse) {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.Lock()
	defer hc.statusLock.Unlock()
	// Set the health status.
	hc.currentDataStatus = status
}

// startHealthCheck starts the health check for the pipeline.
// The ticks are generated at the interval of healthTimeStep.
func (hc *HealthChecker) startHealthCheck(ctx context.Context) {
	logger := logging.FromContext(ctx)
	// Goroutine to listen for ticks
	// At every tick, check and update the health status of the pipeline.
	// If the context is done, return.
	// Create a ticker to generate ticks at the interval of healthTimeStep.
	ticker := time.NewTicker(healthTimeStep)
	defer ticker.Stop()
	for {
		select {
		// If the ticker ticks, check and update the health status of the pipeline.
		case <-ticker.C:
			// Get the current health status of the pipeline.
			criticality, err := hc.getPipelineVertexDataCriticality(ctx)
			logger.Debugw("Health check", zap.Any("criticality", criticality))
			if err != nil {
				// If there is an error, set the current health status to unknown.
				// as we are not able to determine the health of the pipeline.
				logger.Errorw("Failed to vertex data criticality", zap.Error(err))
				hc.setCurrentHealth(defaultDataHealthResponse)
			} else {
				// convert the vertex state to pipeline state
				pipelineState := convertVertexStateToPipelineState(criticality)
				// update the current health status of the pipeline
				hc.setCurrentHealth(pipelineState)
			}
		// If the context is done, return.
		case <-ctx.Done():
			return
		}
	}
}

// getPipelineVertexDataCriticality is used to provide the data criticality of the pipeline
// They can be of the following types:
// 1. Ok: The pipeline is working as expected
// 2. Warning: The pipeline is working but there is a lag in the data movement
// 3. Critical: The pipeline is not working as expected
// We need to check the following things to determine the data criticality of the pipeline:
// 1. The buffer usage of each buffer in the pipeline
//
// Based on this information, we first get the current buffer usage of each buffer in the pipeline at that instant
// and populate the timeline data for each buffer.
// Then for a given buffer, we calculate the weighted mean average buffer usage with decay over
// the last HEALTH_WINDOW_SIZE seconds. This is done to smoothen the curve and remove point spikes
// If this average is greater than the critical threshold, we return the critical status
// If this average is greater than the warning threshold, we return the warning status
// If this average is less than the warning threshold, we return the ok status
func (hc *HealthChecker) getPipelineVertexDataCriticality(ctx context.Context) ([]*vertexState, error) {
	// Fetch the buffer information for the pipeline
	buffers, err := listBuffers(ctx, hc.pipeline, hc.isbSvcClient)
	if err != nil {
		return nil, err
	}
	// update the usage timeline for all the ISBs used in the pipeline
	hc.updateUsageTimeline(buffers.Buffers)

	var vertexState []*vertexState

	// iterate over the timeline data for each buffer and calculate the exponential weighted mean average
	// for the last HEALTH_WINDOW_SIZE buffer usage entries
	for bufferName := range hc.timelineData {
		// Extract the buffer usage of the timeline
		var bufferUsage []float64
		for _, entry := range hc.timelineData[bufferName].Items() {
			bufferUsage = append(bufferUsage, entry.BufferUsage)
		}
		// calculate the average buffer usage over the last HEALTH_WINDOW_SIZE seconds
		ewmaBufferUsage := calculateEWMAUsage(bufferUsage)
		// assign the state to the vertex based on the average buffer usage
		// Look back is enabled for the critical state
		currentState := assignStateToTimeline(ewmaBufferUsage, enableCriticalLookBack)
		// create a new vertex state object
		currentVertexState := newVertexState(bufferName, currentState)
		// add the vertex state to the list of vertex states
		vertexState = append(vertexState, currentVertexState)
	}
	return vertexState, nil
}

// updateUsageTimeline is used to update the usage timeline for a given buffer list
// This iterates over all the buffers in the buffer list and updates the usage timeline for each buffer
// The timeline data is represented as a map of buffer name to a list of timelineEntry
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
			hc.timelineData[bufferName] = sharedqueue.New[*timelineEntry](int(healthWindowSize))
		}
		// extract the current buffer usage and update the average buffer usage
		bufferUsage := buffer.GetBufferUsage().GetValue() * 100
		newAverage := updateAverageBufferUsage(hc.timelineData[bufferName].Items(), bufferUsage)
		// add the new entry to the timeline
		hc.timelineData[bufferName].Append(&timelineEntry{
			Time:               timestamp,
			BufferUsage:        bufferUsage,
			AverageBufferUsage: newAverage,
		})
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
func updateAverageBufferUsage(timeline []*timelineEntry, newEntry float64) float64 {
	// If the timeline is empty, return the buffer usage of the new entry
	if len(timeline) == 0 {
		return newEntry
	}
	// Compute the current average buffer usage of the last entry
	lastAverage := timeline[len(timeline)-1].AverageBufferUsage
	var newAverage float64
	// If the timeline is full
	if len(timeline) == int(healthWindowSize) {
		newAverage = (lastAverage*float64(healthWindowSize) + newEntry - timeline[0].BufferUsage) / float64(healthWindowSize)
	} else if len(timeline) < int(healthWindowSize) {
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
	a := ewma.NewSimpleEWMA(float64(healthWindowSize))
	var emwaValues []float64
	// TODO: Check if we can keep storing the EWMA values instead of recomputing them
	for _, f := range bufferUsage {
		a.Add(f)
		emwaValues = append(emwaValues, a.Get())
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
	if ewmaValue > criticalBufferThreshold {
		state = criticalState
	} else if ewmaValue > warningBufferThreshold {
		state = warningState
	} else {
		state = healthyState
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
	if state == criticalState && lookBack {
		// Extract the states of the timeline
		var states []string
		for _, entry := range ewmaValues {
			states = append(states, assignStateToBufferUsage(entry))
		}
		// Count the number of times the state is CRITICAL in the last CRITICAL_WINDOW_SIZE entries
		var criticalCount int
		for i := len(states) - 1; i >= 0; i-- {
			if states[i] == criticalState {
				criticalCount++
			}
			if (len(states) - i) == criticalWindowSize {
				break
			}
		}
		// If the state is CRITICAL at least LOOK_BACK_COUNT times in the last CRITICAL_WINDOW_SIZE entries
		// Set the state to CRITICAL, otherwise set the state to WARNING
		if criticalCount >= criticalLookBackCount {
			state = criticalState
		} else {
			state = warningState
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
func convertVertexStateToPipelineState(vertexState []*vertexState) *dataHealthResponse {
	// create a map to store the precedence order of the states
	// assign a number to each state based on the precedence order

	// unknown > critical > warning > ok
	stateMap := map[string]int{
		v1alpha1.PipelineStatusUnknown:  3,
		v1alpha1.PipelineStatusCritical: 2,
		v1alpha1.PipelineStatusWarning:  1,
		v1alpha1.PipelineStatusHealthy:  0,
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
	return newDataHealthResponse(v1alpha1.PipelineStatusUnknown,
		"Pipeline data flow is in an unknown state",
		"D4")
}

// generateDataHealthResponse is used to generate the data health response
// if the state is Healthy we return a message saying the pipeline is healthy, and the code corresponding to Healthy
// if the state is Warning we return a message saying the pipeline is warning due to the vertex,
// and the code corresponding to Warning, similar for Critical
// if the state is Unknown we return a message saying the pipeline is in an unknown state due to the vertex,
// and the code corresponding to Unknown
func generateDataHealthResponse(state string, vertex string) *dataHealthResponse {
	switch state {
	case v1alpha1.PipelineStatusHealthy:
		return newDataHealthResponse(
			v1alpha1.PipelineStatusHealthy,
			"Pipeline data flow is healthy",
			"D1")
	case v1alpha1.PipelineStatusWarning:
		return newDataHealthResponse(
			v1alpha1.PipelineStatusWarning,
			fmt.Sprintf("Pipeline data flow is in a warning state for %s", vertex),
			"D2")
	case v1alpha1.PipelineStatusCritical:
		return newDataHealthResponse(
			v1alpha1.PipelineStatusCritical,
			fmt.Sprintf("Pipeline data flow is in a critical state for %s", vertex),
			"D3")
	case v1alpha1.PipelineStatusUnknown:
		return newDataHealthResponse(
			v1alpha1.PipelineStatusUnknown,
			fmt.Sprintf("Pipeline data flow is in an unknown state due to %s", vertex),
			"D4")
	default:
		return defaultDataHealthResponse
	}
}

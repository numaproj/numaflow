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
	"math"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

const (
	// healthTimeStep is the frequency at which the health of a MonoVertex is computed
	healthTimeStep = 30 * time.Second
)

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
var defaultDataHealthResponse = newDataHealthResponse(
	v1alpha1.MonoVertexStatusUnknown,
	"MonoVertex data flow is in an unknown state",
	"D4")

// HealthChecker is the struct type for health checker.
type HealthChecker struct {
	// field for the health status.
	currentDataStatus *dataHealthResponse
	// Spec of the monoVertex getting processed
	monoVertex *v1alpha1.MonoVertex
	// Mutex for state information
	statusLock *sync.RWMutex
}

// NewHealthChecker creates a new object HealthChecker struct type.
func NewHealthChecker(monoVertex *v1alpha1.MonoVertex) *HealthChecker {
	// Return a new HealthChecker struct instance.
	return &HealthChecker{
		currentDataStatus: defaultDataHealthResponse,
		monoVertex:        monoVertex,
		statusLock:        &sync.RWMutex{},
	}
}

// monoVtxState is a struct which contains the name and data health state
// of a MonoVertex
type monoVtxState struct {
	// Name is the name of the vertex
	Name string `json:"name"`
	// State is the state of the vertex
	State string `json:"state"`
}

// newMonoVtxState is used to create a new monoVtxState object
func newMonoVtxState(name string, state string) *monoVtxState {
	return &monoVtxState{
		Name:  name,
		State: state,
	}
}

// getCurrentHealth returns the current health status of the MonoVertex.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) getCurrentHealth() *dataHealthResponse {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.RLock()
	defer hc.statusLock.RUnlock()
	// Return the health status.
	return hc.currentDataStatus
}

// setCurrentHealth sets the current health status of the MonoVertex.
// It is thread safe to ensure concurrent access.
func (hc *HealthChecker) setCurrentHealth(status *dataHealthResponse) {
	// Lock the statusLock to ensure thread safety.
	hc.statusLock.Lock()
	defer hc.statusLock.Unlock()
	// Set the health status.
	hc.currentDataStatus = status
}

// getMonoVertexDataCriticality is used to provide the data criticality of the MonoVertex.
// They can be of the following types:
// 1. Healthy: backlog can be cleared with the currently active replicas
// 2. Warning: backlog suggests more capacity than the currently active replicas
//
// activePods is the live count from the rater pod tracker (DNS-based discovery).
func (hc *HealthChecker) getMonoVertexDataCriticality(_ context.Context, mvtxMetrics *mvtxdaemon.MonoVertexMetrics, activePods int) (*monoVtxState, error) {
	desiredReplicas, err := hc.getDesiredReplica(mvtxMetrics, activePods)
	if err != nil {
		return nil, err
	}
	status := v1alpha1.MonoVertexStatusHealthy
	if desiredReplicas > activePods {
		status = v1alpha1.MonoVertexStatusWarning
	}
	return newMonoVtxState(mvtxMetrics.MonoVertex, status), nil
}

// getDesiredReplica calculates the desired replicas based on the processing rate and pending information
// of the MonoVertex. This logic is similar to our scaling logic.
// But unlike the scaling where we change the desired replicas based on the provided scale,
// here we just calculate the desired replicas and return it.
func (hc *HealthChecker) getDesiredReplica(mvtxMetrics *mvtxdaemon.MonoVertexMetrics, activePods int) (int, error) {
	totalRate := float64(0)
	totalPending := int64(0)
	// Extract the processing rate from the metrics for the default lookback period
	rate, existing := mvtxMetrics.ProcessingRates["default"]
	// Rate not available
	// send back error that we cannot calculate the health status right now
	if !existing || rate.GetValue() < 0 {
		return 0, fmt.Errorf("cannot check data health, MonoVertex %s has no rate information", mvtxMetrics.MonoVertex)
	} else {
		totalRate = rate.GetValue()
	}

	// Extract the pending information from the metrics for the default lookback period
	pending, existing := mvtxMetrics.Pendings["default"]
	if !existing || pending.GetValue() < 0 || pending.GetValue() == v1alpha1.PendingNotAvailable {
		// Pending not available, we don't do anything
		// send back error that we calculate the health status right now
		return 0, fmt.Errorf("cannot check data health, MonoVertex %s has no pending information", mvtxMetrics.MonoVertex)
	} else {
		totalPending = pending.GetValue()
	}

	// If both totalRate and totalPending are 0, we don't need any processing replicas
	if totalPending == 0 && totalRate == 0 {
		return 0, nil
	}

	//TODO(MonoVertex): Something is wrong
	// MonoVertex is not processing any data even though the pending is still around.
	// It could be a slow processor, but zero rate isn't ideal
	// we should mark this up as warning maybe?
	if totalRate == 0 {
		return activePods, nil
	}

	// We calculate the time of finishing processing the pending messages,
	// and then we know how many replicas are needed to get them done in target seconds.
	desired := int32(math.Round(((float64(totalPending) / totalRate) / float64(hc.monoVertex.Spec.Scale.GetTargetProcessingSeconds())) * float64(activePods)))
	return int(desired), nil
}

// convertMonoVtxStateToHealthResp is used to generate the data health response from a MonoVtx State
func (hc *HealthChecker) convertMonoVtxStateToHealthResp(vertexState *monoVtxState) *dataHealthResponse {
	switch vertexState.State {
	case v1alpha1.MonoVertexStatusHealthy:
		return newDataHealthResponse(
			v1alpha1.MonoVertexStatusHealthy,
			"MonoVertex data flow is healthy",
			"D1")
	case v1alpha1.MonoVertexStatusWarning:
		return newDataHealthResponse(
			v1alpha1.MonoVertexStatusWarning,
			fmt.Sprintf("MonoVertex data flow is in a warning state for %s", vertexState.Name),
			"D2")
	case v1alpha1.MonoVertexStatusCritical:
		return newDataHealthResponse(
			v1alpha1.MonoVertexStatusCritical,
			fmt.Sprintf("MonoVertex data flow is in a critical state for %s", vertexState.Name),
			"D3")
	case v1alpha1.MonoVertexStatusUnknown:
		return newDataHealthResponse(
			v1alpha1.MonoVertexStatusUnknown,
			fmt.Sprintf("MonoVertex data flow is in an unknown state due to %s", vertexState.Name),
			"D4")
	default:
		return defaultDataHealthResponse
	}
}

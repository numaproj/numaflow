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
	"crypto/tls"
	"net/http"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	raterPkg "github.com/numaproj/numaflow/pkg/mvtxdaemon/server/service/rater"
	runtimePkg "github.com/numaproj/numaflow/pkg/mvtxdaemon/server/service/runtime"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MonoVtxPendingMetric is the metric emitted from the MonoVtx lag reader for pending stats
// Note: Please keep consistent with the definitions in rust/monovertex/sc/metrics.rs
const MonoVtxPendingMetric = "monovtx_pending"

type MonoVertexService struct {
	mvtxdaemon.UnimplementedMonoVertexDaemonServiceServer
	monoVtx                *v1alpha1.MonoVertex
	httpClient             *http.Client
	rater                  raterPkg.MonoVtxRatable
	healthChecker          *HealthChecker
	monoVertexRuntimeCache runtimePkg.MonoVertexRuntimeCache
}

var _ mvtxdaemon.MonoVertexDaemonServiceServer = (*MonoVertexService)(nil)

// NewMoveVertexService returns a new instance of MonoVertexService
func NewMoveVertexService(
	monoVtx *v1alpha1.MonoVertex,
	rater raterPkg.MonoVtxRatable,
	monoVertexRuntimeCache runtimePkg.MonoVertexRuntimeCache,
) (*MonoVertexService, error) {
	mv := MonoVertexService{
		monoVtx: monoVtx,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		rater:                  rater,
		healthChecker:          NewHealthChecker(monoVtx),
		monoVertexRuntimeCache: monoVertexRuntimeCache,
	}
	return &mv, nil
}

func (mvs *MonoVertexService) GetMonoVertexMetrics(ctx context.Context, empty *emptypb.Empty) (*mvtxdaemon.GetMonoVertexMetricsResponse, error) {
	return mvs.fetchMonoVertexMetrics(ctx)
}

// fetchMonoVertexMetrics is a helper function to derive the MonoVertex metrics
func (mvs *MonoVertexService) fetchMonoVertexMetrics(ctx context.Context) (*mvtxdaemon.GetMonoVertexMetricsResponse, error) {
	resp := new(mvtxdaemon.GetMonoVertexMetricsResponse)
	collectedMetrics := new(mvtxdaemon.MonoVertexMetrics)
	collectedMetrics.MonoVertex = mvs.monoVtx.Name
	collectedMetrics.Pendings = mvs.rater.GetPending()
	collectedMetrics.ProcessingRates = mvs.rater.GetRates()
	resp.Metrics = collectedMetrics
	return resp, nil
}

func (mvs *MonoVertexService) GetMonoVertexStatus(ctx context.Context, empty *emptypb.Empty) (*mvtxdaemon.GetMonoVertexStatusResponse, error) {
	resp := new(mvtxdaemon.GetMonoVertexStatusResponse)
	collectedStatus := new(mvtxdaemon.MonoVertexStatus)
	dataHealth := mvs.healthChecker.getCurrentHealth()
	collectedStatus.Status = dataHealth.Status
	collectedStatus.Message = dataHealth.Message
	collectedStatus.Code = dataHealth.Code
	resp.Status = collectedStatus
	return resp, nil
}

// GetMonoVertexErrors returns errors for a given mono vertex by accessing the local cache in the runtime service.
// The errors are persisted in the local cache by the runtime service.
// Errors are retrieved for all active replicas for a given mono vertex.
// A list of replica errors for a given mono vertex is returned.
func (mvs *MonoVertexService) GetMonoVertexErrors(ctx context.Context, request *mvtxdaemon.GetMonoVertexErrorsRequest) (*mvtxdaemon.GetMonoVertexErrorsResponse, error) {
	monoVertex := request.GetMonoVertex()
	resp := new(mvtxdaemon.GetMonoVertexErrorsResponse)
	localCache := mvs.monoVertexRuntimeCache.GetLocalCache()

	// If the errors are present in the local cache, return the errors.
	if errors, ok := localCache[monoVertex]; ok {
		replicaErrors := make([]*mvtxdaemon.ReplicaErrors, len(errors))
		for i, err := range errors {
			containerErrors := make([]*mvtxdaemon.ContainerError, len(err.ContainerErrors))
			for j, containerError := range err.ContainerErrors {
				containerErrors[j] = &mvtxdaemon.ContainerError{
					Container: containerError.Container,
					Timestamp: timestamppb.New(time.Unix(containerError.Timestamp, 0)),
					Code:      containerError.Code,
					Message:   containerError.Message,
					Details:   containerError.Details,
				}
			}
			replicaErrors[i] = &mvtxdaemon.ReplicaErrors{
				Replica:         err.Replica,
				ContainerErrors: containerErrors,
			}
		}
		resp.Errors = replicaErrors
	}

	return resp, nil
}

// StartHealthCheck starts the health check for the MonoVertex using the health checker
func (mvs *MonoVertexService) StartHealthCheck(ctx context.Context) {
	mvs.startHealthCheck(ctx)
}

// startHealthCheck starts the health check for the pipeline.
// The ticks are generated at the interval of healthTimeStep.
func (mvs *MonoVertexService) startHealthCheck(ctx context.Context) {
	logger := logging.FromContext(ctx)
	// Goroutine to listen for ticks
	// At every tick, check and update the health status of the MonoVertex.
	// If the context is done, return.
	// Create a ticker to generate ticks at the interval of healthTimeStep.
	ticker := time.NewTicker(healthTimeStep)
	defer ticker.Stop()
	for {
		select {
		// Get the current health status of the MonoVertex.
		case <-ticker.C:
			// Fetch the MonoVertex metrics, these are required for deriving the
			// health status
			mvtxMetrics, _ := mvs.fetchMonoVertexMetrics(ctx)
			// Calculate the data criticality
			criticality, err := mvs.healthChecker.getMonoVertexDataCriticality(ctx, mvtxMetrics.Metrics)
			logger.Debugw("MonoVertex Health check", zap.Any("criticality", criticality))
			if err != nil {
				// Warn level is used to avoid flooding the logs with errors
				// in case there are 0 pods running.
				logger.Warnw("Failed to check MonoVertex data criticality, perhaps there are no running pods!", zap.Error(err))
				// If there is an error, set the current health status to unknown.
				// as we are not able to determine the health of the MonoVertex.
				mvs.healthChecker.setCurrentHealth(defaultDataHealthResponse)
			} else {
				// convert the MonoVertex health state to API response
				monoVertexState := mvs.healthChecker.convertMonoVtxStateToHealthResp(criticality)
				// update the current health status of the MonoVertex to cache
				mvs.healthChecker.setCurrentHealth(monoVertexState)
			}
		// If the context is done, return.
		case <-ctx.Done():
			return
		}
	}
}

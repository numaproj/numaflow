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
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/prometheus/common/expfmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	"github.com/numaproj/numaflow/pkg/metrics"
	raterPkg "github.com/numaproj/numaflow/pkg/mvtxdaemon/server/service/rater"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// MonoVtxPendingMetric is the metric emitted from the MonoVtx lag reader for pending stats
// Note: Please keep consistent with the definitions in rust/monovertex/sc/metrics.rs
const MonoVtxPendingMetric = "monovtx_pending"

type MonoVertexService struct {
	mvtxdaemon.UnimplementedMonoVertexDaemonServiceServer
	monoVtx       *v1alpha1.MonoVertex
	httpClient    *http.Client
	rater         raterPkg.MonoVtxRatable
	healthChecker *HealthChecker
}

var _ mvtxdaemon.MonoVertexDaemonServiceServer = (*MonoVertexService)(nil)

// NewMoveVertexService returns a new instance of MonoVertexService
func NewMoveVertexService(
	monoVtx *v1alpha1.MonoVertex,
	rater raterPkg.MonoVtxRatable,
) (*MonoVertexService, error) {
	mv := MonoVertexService{
		monoVtx: monoVtx,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		rater:         rater,
		healthChecker: NewHealthChecker(monoVtx),
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
	collectedMetrics.Pendings = mvs.getPending(ctx)
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

// getPending returns the pending count for the mono vertex
func (mvs *MonoVertexService) getPending(ctx context.Context) map[string]*wrapperspb.Int64Value {
	log := logging.FromContext(ctx)
	headlessServiceName := mvs.monoVtx.GetHeadlessServiceName()
	pendingMap := make(map[string]*wrapperspb.Int64Value)

	// Get the headless service name
	// We can query the metrics endpoint of the (i)th pod to obtain this value.
	// example for 0th pod : https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless:2469/metrics
	url := fmt.Sprintf("https://%s-mv-0.%s.%s.svc:%v/metrics", mvs.monoVtx.Name, headlessServiceName, mvs.monoVtx.Namespace, v1alpha1.MonoVertexMetricsPort)
	if res, err := mvs.httpClient.Get(url); err != nil {
		log.Debugf("Error reading the metrics endpoint, it might be because of mono vertex scaling down to 0: %f", err.Error())
		return nil
	} else {
		// expfmt Parser from prometheus to parse the metrics
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			log.Errorw("Error in parsing to prometheus metric families", zap.Error(err))
			return nil
		}

		// Get the pending messages
		if value, ok := result[MonoVtxPendingMetric]; ok {
			metricsList := value.GetMetric()
			for _, metric := range metricsList {
				labels := metric.GetLabel()
				lookback := ""
				for _, label := range labels {
					if label.GetName() == metrics.LabelPeriod {
						lookback = label.GetValue()
						break
					}
				}
				pendingMap[lookback] = wrapperspb.Int64(int64(metric.Gauge.GetValue()))
			}
		}
	}
	return pendingMap
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
				// If there is an error, set the current health status to unknown.
				// as we are not able to determine the health of the pipeline.
				logger.Errorw("Failed to MonoVertex data criticality", zap.Error(err))
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

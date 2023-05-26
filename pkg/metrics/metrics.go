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

package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	LabelPipeline           = "pipeline"
	LabelVertex             = "vertex"
	LabelPeriod             = "period"
	LabelVertexReplicaIndex = "replica"

	VertexProcessingRate  = "vertex_processing_rate"
	VertexPendingMessages = "vertex_pending_messages"
)

var (
	processingRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: VertexProcessingRate,
		Help: "Message processing rate in the last period of seconds, tps. It represents the rate of a vertex instead of a pod.",
	}, []string{LabelPipeline, LabelVertex, LabelPeriod})

	pending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: VertexPendingMessages,
		Help: "Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.",
	}, []string{LabelPipeline, LabelVertex, LabelPeriod})

	// fixedLookbackSeconds Always expose metrics of following lookback seconds (1m, 5m, 15m)
	fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}
)

// timestampedPending is a helper struct to wrap a pending number and timestamp pair
type timestampedPending struct {
	pending int64
	// timestamp in seconds
	timestamp int64
}

// metricsServer runs an HTTP server to:
// 1. Expose metrics;
// 2. Serve an endpoint to execute health checks
type metricsServer struct {
	vertex     *dfv1.Vertex
	raters     []isb.Ratable
	lagReaders []isb.LagReader
	// lookbackSeconds is the look back seconds for pending and rate calculation used for autoscaling
	lookbackSeconds     int64
	lagCheckingInterval time.Duration
	refreshInterval     time.Duration
	// pendingInfo stores a list of pending/timestamp(seconds) information
	pendingInfo *sharedqueue.OverflowQueue[timestampedPending]
	// Functions that health check executes
	healthCheckExecutors []func() error
}

type Option func(*metricsServer)

// WithRaters sets the raters
func WithRaters(r []isb.Ratable) Option {
	return func(m *metricsServer) {
		m.raters = r
	}
}

// WithLagReaders sets the lag reader
func WithLagReaders(r []isb.LagReader) Option {
	return func(m *metricsServer) {
		m.lagReaders = r
	}
}

// WithRefreshInterval sets how often to refresh the rate and pending
func WithRefreshInterval(d time.Duration) Option {
	return func(m *metricsServer) {
		m.refreshInterval = d
	}
}

// WithLookbackSeconds sets lookback seconds for avg rate and pending calculation
func WithLookbackSeconds(seconds int64) Option {
	return func(m *metricsServer) {
		m.lookbackSeconds = seconds
	}
}

// WithHealthCheckExecutor appends a health check executor
func WithHealthCheckExecutor(f func() error) Option {
	return func(m *metricsServer) {
		m.healthCheckExecutors = append(m.healthCheckExecutors, f)
	}
}

// NewMetricsOptions returns a metrics option list.
func NewMetricsOptions(ctx context.Context, vertex *dfv1.Vertex, serverHandler HealthChecker, readers []isb.BufferReader, writers []isb.BufferWriter) []Option {
	metricsOpts := []Option{
		WithLookbackSeconds(int64(vertex.Spec.Scale.GetLookbackSeconds())),
	}
	if serverHandler != nil {
		if util.LookupEnvStringOr(dfv1.EnvHealthCheckDisabled, "false") != "true" {
			metricsOpts = append(metricsOpts, WithHealthCheckExecutor(func() error {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				return serverHandler.IsHealthy(cctx)
			}))
		}
	}
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		if x, ok := reader.(isb.LagReader); ok {
			lagReaders = append(lagReaders, x)
		}
	}
	if len(lagReaders) > 0 {
		metricsOpts = append(metricsOpts, WithLagReaders(lagReaders))
	}
	var raters []isb.Ratable
	if vertex.IsASource() {
		for _, writer := range writers {
			if x, ok := writer.(isb.Ratable); ok {
				raters = append(raters, x)
			}
		}
		if len(raters) > 0 {
			metricsOpts = append(metricsOpts, WithRaters(raters))
		}
	} else {
		for _, reader := range readers {
			if x, ok := reader.(isb.Ratable); ok {
				raters = append(raters, x)
			}
		}
		if len(raters) > 0 {
			metricsOpts = append(metricsOpts, WithRaters(raters))
		}
	}
	return metricsOpts
}

// NewMetricsServer returns a Prometheus metrics server instance, which can be used to start an HTTPS service to expose Prometheus metrics.
func NewMetricsServer(vertex *dfv1.Vertex, opts ...Option) *metricsServer {
	m := new(metricsServer)
	m.vertex = vertex
	m.refreshInterval = 5 * time.Second             // Default refresh interval
	m.lagCheckingInterval = 3 * time.Second         // Default lag checking interval
	m.lookbackSeconds = dfv1.DefaultLookbackSeconds // Default
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.lagReaders != nil {
		m.pendingInfo = sharedqueue.New[timestampedPending](1800)
	}
	return m
}

// Enqueue pending pending information
func (ms *metricsServer) buildupPendingInfo(ctx context.Context) {
	if ms.lagReaders == nil {
		return
	}
	log := logging.FromContext(ctx)
	ticker := time.NewTicker(ms.lagCheckingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			totalPending := int64(0)
			for _, pending := range ms.lagReaders {
				if p, err := pending.Pending(ctx); err != nil {
					log.Errorw("Failed to get pending messages", zap.Error(err))
				} else {
					totalPending += p
				}
			}

			if totalPending != isb.PendingNotAvailable {
				ts := timestampedPending{pending: totalPending, timestamp: time.Now().Unix()}
				ms.pendingInfo.Append(ts)
			}
		}
	}
}

// Expose pending and rate metrics
func (ms *metricsServer) exposePendingAndRate(ctx context.Context) {
	if ms.lagReaders == nil && ms.raters == nil {
		return
	}
	log := logging.FromContext(ctx)
	lookbackSecondsMap := map[string]int64{"default": ms.lookbackSeconds} // Metrics for autoscaling use key "default"
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	ticker := time.NewTicker(ms.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ms.raters != nil {
				for n, i := range lookbackSecondsMap {
					totalRate := float64(0)
					for _, rater := range ms.raters {
						if r, err := rater.Rate(ctx, i); err != nil {
							log.Errorw("Failed to get processing rate in the past seconds", zap.Int64("seconds", i), zap.Error(err))
						} else {
							totalRate += r
						}
					}
					totalRate = totalRate / float64(len(ms.raters))
					if totalRate != isb.RateNotAvailable {
						processingRate.WithLabelValues(ms.vertex.Spec.PipelineName, ms.vertex.Spec.Name, n).Set(totalRate)
					}
				}
			}
			if ms.lagReaders != nil {
				for n, i := range lookbackSecondsMap {
					if p := ms.calculatePending(i); p != isb.PendingNotAvailable {
						pending.WithLabelValues(ms.vertex.Spec.PipelineName, ms.vertex.Spec.Name, n).Set(float64(p))
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Calculate the avg pending of last seconds
func (ms *metricsServer) calculatePending(seconds int64) int64 {
	result := isb.PendingNotAvailable
	items := ms.pendingInfo.Items()
	total := int64(0)
	num := int64(0)
	now := time.Now().Unix()
	for i := len(items) - 1; i >= 0; i-- {
		if now-items[i].timestamp < seconds {
			total += items[i].pending
			num++
		} else {
			break
		}
	}
	if num > 0 {
		result = total / num
	}
	return result
}

// Start function starts the HTTPS service to expose metrics, it returns a shutdown function and an error if any
func (ms *metricsServer) Start(ctx context.Context) (func(ctx context.Context) error, error) {
	log := logging.FromContext(ctx)
	log.Info("Generating self-signed certificate")
	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate cert: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/sidecar-livez", func(w http.ResponseWriter, r *http.Request) {
		if len(ms.healthCheckExecutors) > 0 {
			for _, ex := range ms.healthCheckExecutors {
				if err := ex(); err != nil {
					log.Errorw("Failed to execute sidecar health check", zap.Error(err))
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(err.Error()))
					return
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	pprofEnabled := os.Getenv(dfv1.EnvDebug) == "true" || os.Getenv(dfv1.EnvPPROF) == "true"
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	} else {
		log.Info("Not enabling pprof debug endpoints")
	}

	httpServer := &http.Server{
		Addr:      fmt.Sprintf(":%d", dfv1.VertexMetricsPort),
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12},
	}
	// Buildup pending information
	go ms.buildupPendingInfo(ctx)
	// Expose pending and rate metrics
	go ms.exposePendingAndRate(ctx)
	go func() {
		log.Info("Starting metrics HTTPS server")
		if err := httpServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			log.Fatalw("Failed to listen-and-server on HTTPS", zap.Error(err))
		}
		log.Info("Metrics server shutdown")
	}()
	return httpServer.Shutdown, nil
}

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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// metricsServer runs an HTTP server to:
// 1. Expose metrics;
// 2. Serve an endpoint to execute health checks
type metricsServer struct {
	vertex     *dfv1.Vertex
	lagReaders map[string]isb.LagReader
	// lookbackSeconds is the look back seconds for pending calculation used for autoscaling
	lookbackSeconds     int64
	lagCheckingInterval time.Duration
	refreshInterval     time.Duration
	// Functions that health check executes
	healthCheckExecutors []func() error
}

type Option func(*metricsServer)

// WithLagReaders sets the lag readers
func WithLagReaders(r map[string]isb.LagReader) Option {
	return func(m *metricsServer) {
		m.lagReaders = r
	}
}

// WithRefreshInterval sets how often to refresh the pending information
func WithRefreshInterval(d time.Duration) Option {
	return func(m *metricsServer) {
		m.refreshInterval = d
	}
}

// WithLookbackSeconds sets lookback seconds for pending calculation
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
func NewMetricsOptions(ctx context.Context, vertex *dfv1.Vertex, healthCheckers []HealthChecker, readers []isb.LagReader) []Option {
	metricsOpts := []Option{
		WithLookbackSeconds(int64(vertex.Spec.Scale.GetLookbackSeconds())),
	}

	if util.LookupEnvStringOr(dfv1.EnvHealthCheckDisabled, "false") != "true" {
		for _, hc := range healthCheckers {
			metricsOpts = append(metricsOpts, WithHealthCheckExecutor(func() error {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				return hc.IsHealthy(cctx)
			}))
		}
	}

	lagReaders := make(map[string]isb.LagReader)
	for _, reader := range readers {
		lagReaders[reader.GetName()] = reader
	}
	if len(lagReaders) > 0 {
		metricsOpts = append(metricsOpts, WithLagReaders(lagReaders))
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
	return m
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
		for _, ex := range ms.healthCheckExecutors {
			if err := ex(); err != nil {
				log.Errorw("Failed to execute sidecar health check", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/sidecar-livez", func(w http.ResponseWriter, r *http.Request) {
		for _, ex := range ms.healthCheckExecutors {
			if err := ex(); err != nil {
				log.Errorw("Failed to execute sidecar health check", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
				return
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

	go func() {
		log.Info("Starting metrics HTTPS server")
		if err := httpServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			log.Fatalw("Failed to listen-and-server on HTTPS", zap.Error(err))
		}
		log.Info("Metrics server shutdown")
	}()
	return httpServer.Shutdown, nil
}

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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/numaproj/numaflow"
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/daemon/server/service"
	server "github.com/numaproj/numaflow/pkg/daemon/server/service/rater"
	runtimeinfo "github.com/numaproj/numaflow/pkg/daemon/server/service/runtime"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/telemetry"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/prometheus/client_golang/prometheus"
)

type daemonServer struct {
	pipeline      *v1alpha1.Pipeline
	isbSvcType    v1alpha1.ISBSvcType
	metaDataQuery *service.PipelineMetadataQuery
}

func NewDaemonServer(pl *v1alpha1.Pipeline, isbSvcType v1alpha1.ISBSvcType) *daemonServer {
	return &daemonServer{
		pipeline:      pl,
		isbSvcType:    isbSvcType,
		metaDataQuery: nil,
	}
}

func (ds *daemonServer) Run(ctx context.Context) error {
	log := logging.FromContext(ctx)
	var (
		isbSvcClient   isbsvc.ISBService
		err            error
		natsClientPool *jsclient.ClientPool
	)

	switch ds.isbSvcType {
	case v1alpha1.ISBSvcTypeJetStream:
		natsClientPool, err = jsclient.NewClientPool(ctx, jsclient.WithClientPoolSize(1))
		if err != nil {
			log.Errorw("Failed to get a NATS client pool.", zap.Error(err))
			return err
		}
		defer natsClientPool.CloseAll()
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(natsClientPool.NextAvailableClient())
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unsupported isbsvc buffer type %q", ds.isbSvcType)
	}

	// rater is used to calculate the processing rate for each of the vertices
	rater := server.NewRater(ctx, ds.pipeline)
	// pipelineRuntimeCache is used to cache and retrieve the runtime errors
	pipelineRuntimeCache := runtimeinfo.NewRuntime(ctx, ds.pipeline)

	// Start listener
	var conn net.Listener
	var listerErr error
	address := fmt.Sprintf(":%d", v1alpha1.DaemonServicePort)
	conn, err = net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", listerErr)
	}

	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate cert: %w", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1", "h2"}}
	grpcServer, err := ds.newGRPCServer(ctx, isbSvcClient, rater, pipelineRuntimeCache)
	if err != nil {
		return fmt.Errorf("failed to create grpc server: %w", err)
	}

	// Clean up watermark service when daemon server exits
	defer func() {
		if ds.metaDataQuery != nil {
			ds.metaDataQuery.Stop()
		}
	}()
	httpServer := ds.newHTTPServer(ctx, v1alpha1.DaemonServicePort, tlsConfig)

	conn = tls.NewListener(conn, tlsConfig)
	// Cmux is used to support servicing gRPC and HTTP1.1+JSON on the same port
	tcpm := cmux.New(conn)
	httpL := tcpm.Match(cmux.HTTP1Fast())
	grpcL := tcpm.Match(cmux.Any())

	go func() { _ = grpcServer.Serve(grpcL) }()
	go func() { _ = httpServer.Serve(httpL) }()
	go func() { _ = tcpm.Serve() }()

	// Start the Data flow health status updater
	go func() {
		ds.metaDataQuery.StartHealthCheck(ctx)
	}()

	// Start the rater
	go func() {
		if err := rater.Start(ctx); err != nil {
			log.Panic(fmt.Errorf("failed to start the rater: %w", err))
		}
	}()

	// Start the pipeline runtime cache refresher
	go func() {
		if err := pipelineRuntimeCache.StartCacheRefresher(ctx); err != nil {
			log.Panic(fmt.Errorf("failed to start the pipeline runtime cache refresher: %w", err))
		}
	}()

	go ds.exposeMetrics(ctx)

	// Initialize OTLP exporter if OTEL_EXPORTER_OTLP_ENDPOINT is set
	shutdown, err := telemetry.InitOTLPExporter(ctx, v1alpha1.ComponentDaemon, ds.pipeline.Name, prometheus.DefaultGatherer)
	if err != nil {
		log.Warnw("Failed to initialize OTLP exporter, continuing without OTLP", zap.Error(err))
	} else {
		defer func() {
			_ = shutdown(context.Background())
		}()
	}

	version := numaflow.GetVersion()
	metrics.BuildInfo.WithLabelValues(v1alpha1.ComponentDaemon, ds.pipeline.Name, version.Version, version.Platform).Set(1)

	log.Infof("Daemon server started successfully on %s", address)
	<-ctx.Done()
	return nil
}

func (ds *daemonServer) newGRPCServer(
	ctx context.Context,
	isbSvcClient isbsvc.ISBService,
	rater server.Ratable,
	pipelineRuntimeCache runtimeinfo.PipelineRuntimeCache) (*grpc.Server, error) {
	// "Prometheus histograms are a great way to measure latency distributions of your RPCs.
	// However, since it is a bad practice to have metrics of high cardinality the latency monitoring metrics are disabled by default.
	// To enable them please call the following in your server initialization code:"
	grpc_prometheus.EnableHandlingTimeHistogram()

	sOpts := []grpc.ServerOption{
		grpc.ConnectionTimeout(300 * time.Second),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
		)),
	}
	grpcServer := grpc.NewServer(sOpts...)
	grpc_prometheus.Register(grpcServer)
	pipelineMetadataQuery, err := service.NewPipelineMetadataQuery(ctx, isbSvcClient, ds.pipeline, rater, pipelineRuntimeCache)
	if err != nil {
		return nil, err
	}
	daemon.RegisterDaemonServiceServer(grpcServer, pipelineMetadataQuery)
	ds.metaDataQuery = pipelineMetadataQuery
	return grpcServer, nil
}

// newHTTPServer returns the HTTP server to serve HTTP/HTTPS requests. This is implemented
// using grpc-gateway as a proxy to the gRPC server.
func (ds *daemonServer) newHTTPServer(ctx context.Context, port int, tlsConfig *tls.Config) *http.Server {
	log := logging.FromContext(ctx)
	endpoint := fmt.Sprintf(":%d", port)
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
	}
	gwMuxOpts := runtime.WithMarshalerOption(runtime.MIMEWildcard, new(runtime.JSONPb))
	gwmux := runtime.NewServeMux(gwMuxOpts,
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if key == "Connection" {
				// Remove "Connection: keep-alive", which is always included in the header of a browser access,
				// it will cause "500 Internal Server Error caused by: stream error: stream ID 19; PROTOCOL_ERROR"
				return key, false
			}
			return key, true
		}),
	)
	if err := daemon.RegisterDaemonServiceHandlerFromEndpoint(ctx, gwmux, endpoint, dialOpts); err != nil {
		log.Errorw("Failed to register daemon handler on HTTP Server", zap.Error(err))
	}
	mux := http.NewServeMux()
	httpServer := http.Server{
		Addr:      endpoint,
		Handler:   mux,
		TLSConfig: tlsConfig,
	}
	mux.Handle("/api/", gwmux)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/metrics", promhttp.Handler())
	pprofEnabled := os.Getenv(v1alpha1.EnvDebug) == "true" || os.Getenv(v1alpha1.EnvPPROF) == "true"
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	} else {
		log.Info("Not enabling pprof debug endpoints")
	}

	return &httpServer
}

// calculate processing lag and watermark_delay to current time using watermark values.
func (ds *daemonServer) exposeMetrics(ctx context.Context) {
	log := logging.FromContext(ctx)
	var (
		source = make(map[string]bool)
		sink   = make(map[string]bool)
	)
	for _, vertex := range ds.pipeline.Spec.Vertices {
		if vertex.IsASource() {
			source[vertex.Name] = true
		} else if vertex.IsASink() {
			sink[vertex.Name] = true
		}
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			resp, err := ds.metaDataQuery.GetPipelineWatermarks(ctx, &daemon.GetPipelineWatermarksRequest{Pipeline: ds.pipeline.Name})
			if err != nil {
				log.Errorw("Failed to calculate processing lag for pipeline", zap.Error(err))
				continue
			}
			watermarks := resp.PipelineWatermarks
			var (
				minWM int64 = math.MaxInt64
				maxWM int64 = math.MinInt64
			)

			for _, watermark := range watermarks {
				// find the largest source vertex watermark
				if _, ok := source[watermark.From]; ok {
					for _, wm := range watermark.Watermarks {
						if wm.GetValue() > maxWM {
							maxWM = wm.GetValue()
						}
					}
				}
				// find the smallest sink vertex watermark
				if _, ok := sink[watermark.To]; ok {
					for _, wm := range watermark.Watermarks {
						if wm.GetValue() < minWM {
							minWM = wm.GetValue()
						}
					}
				}
			}

			//exposing pipeline processing lag metric.
			if minWM < 0 {
				metrics.PipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(-1)
			} else {
				if maxWM < minWM {
					metrics.PipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(-1)
				} else {
					metrics.PipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(float64(maxWM - minWM))
				}
			}

			// exposing the watermark delay to current time metric.
			if maxWM == math.MinInt64 {
				metrics.WatermarkCmpNow.WithLabelValues(ds.pipeline.Name).Set(-1)
			} else {
				metrics.WatermarkCmpNow.WithLabelValues(ds.pipeline.Name).Set(float64(time.Now().UnixMilli() - maxWM))
			}

			//exposing Pipeline data processing health metric.
			pipelineDataHealth, err := ds.metaDataQuery.GetPipelineStatus(ctx, &daemon.GetPipelineStatusRequest{Pipeline: ds.pipeline.Name})

			if err != nil {
				log.Errorw("Failed to get data processing health status", zap.Error(err))
				continue
			}
			switch pipelineDataHealth.Status.Status {
			case v1alpha1.PipelineStatusHealthy:
				metrics.DataProcessingHealth.WithLabelValues(ds.pipeline.Name).Set(1)
			case v1alpha1.PipelineStatusWarning:
				metrics.DataProcessingHealth.WithLabelValues(ds.pipeline.Name).Set(-1)
			case v1alpha1.PipelineStatusCritical:
				metrics.DataProcessingHealth.WithLabelValues(ds.pipeline.Name).Set(-2)
			default:
				metrics.DataProcessingHealth.WithLabelValues(ds.pipeline.Name).Set(0)
			}

		case <-ctx.Done():
			return
		}
	}
}

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
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	"github.com/numaproj/numaflow/pkg/mvtxdaemon/server/service"
	rateServer "github.com/numaproj/numaflow/pkg/mvtxdaemon/server/service/rater"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
)

type daemonServer struct {
	monoVtx     *v1alpha1.MonoVertex
	mvtxService *service.MonoVertexService
}

func NewDaemonServer(monoVtx *v1alpha1.MonoVertex) *daemonServer {
	return &daemonServer{
		monoVtx:     monoVtx,
		mvtxService: nil,
	}
}

func (ds *daemonServer) Run(ctx context.Context) error {
	log := logging.FromContext(ctx)
	var (
		err error
	)
	// rater is used to calculate the processing rate of the mono vertex
	rater := rateServer.NewRater(ctx, ds.monoVtx)

	// Start listener
	var conn net.Listener
	var listerErr error
	address := fmt.Sprintf(":%d", v1alpha1.MonoVertexDaemonServicePort)
	conn, err = net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", listerErr)
	}

	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate cert: %w", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12}
	grpcServer, err := ds.newGRPCServer(rater)
	if err != nil {
		return fmt.Errorf("failed to create grpc server: %w", err)
	}
	httpServer := ds.newHTTPServer(ctx, v1alpha1.MonoVertexDaemonServicePort, tlsConfig)

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
		ds.mvtxService.StartHealthCheck(ctx)
	}()

	// Start the rater
	go func() {
		if err := rater.Start(ctx); err != nil {
			log.Panic(fmt.Errorf("failed to start the rater: %w", err))
		}
	}()

	version := numaflow.GetVersion()
	monoVertexInfo.WithLabelValues(version.Version, version.Platform, ds.monoVtx.Name).Set(1)

	log.Infof("MonoVertex daemon server started successfully on %s", address)
	<-ctx.Done()
	return nil
}

func (ds *daemonServer) newGRPCServer(rater rateServer.MonoVtxRatable) (*grpc.Server, error) {
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
	mvtxService, err := service.NewMoveVertexService(ds.monoVtx, rater)
	if err != nil {
		return nil, err
	}
	mvtxdaemon.RegisterMonoVertexDaemonServiceServer(grpcServer, mvtxService)
	ds.mvtxService = mvtxService
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
	if err := mvtxdaemon.RegisterMonoVertexDaemonServiceHandlerFromEndpoint(ctx, gwmux, endpoint, dialOpts); err != nil {
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

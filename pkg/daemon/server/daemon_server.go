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
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/daemon/server/service"
	server "github.com/numaproj/numaflow/pkg/daemon/server/service/rater"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
)

type daemonServer struct {
	pipeline   *v1alpha1.Pipeline
	isbSvcType v1alpha1.ISBSvcType
}

func NewDaemonServer(pl *v1alpha1.Pipeline, isbSvcType v1alpha1.ISBSvcType) *daemonServer {
	return &daemonServer{
		pipeline:   pl,
		isbSvcType: isbSvcType,
	}
}

func (ds *daemonServer) Run(ctx context.Context) error {
	log := logging.FromContext(ctx)
	var (
		isbSvcClient   isbsvc.ISBService
		err            error
		natsClientPool *jsclient.ClientPool
	)

	natsClientPool, err = jsclient.NewClientPool(ctx, jsclient.WithClientPoolSize(1))
	defer natsClientPool.CloseAll()

	if err != nil {
		log.Errorw("Failed to get a NATS client pool.", zap.Error(err))
		return err
	}
	switch ds.isbSvcType {
	case v1alpha1.ISBSvcTypeRedis:
		isbSvcClient = isbsvc.NewISBRedisSvc(redisclient.NewInClusterRedisClient())
	case v1alpha1.ISBSvcTypeJetStream:
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(ds.pipeline.Name, isbsvc.WithJetStreamClient(natsClientPool.NextAvailableClient()))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unsupported isbsvc buffer type %q", ds.isbSvcType)
	}
	wmStores, err := service.BuildWatermarkStores(ctx, ds.pipeline, isbSvcClient)
	if err != nil {
		return fmt.Errorf("failed to get watermark stores, %w", err)
	}
	wmFetchers, err := service.BuildUXEdgeWatermarkFetchers(ctx, ds.pipeline, wmStores)
	if err != nil {
		return fmt.Errorf("failed to get watermark fetchers, %w", err)
	}

	// Close all the watermark stores when the daemon server exits
	defer func() {
		for _, edgeStores := range wmStores {
			for _, store := range edgeStores {
				_ = store.Close()
			}
		}
	}()

	// rater is used to calculate the processing rate for each of the vertices
	rater := server.NewRater(ctx, ds.pipeline)

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

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12}
	grpcServer, err := ds.newGRPCServer(isbSvcClient, wmFetchers, rater)
	if err != nil {
		return fmt.Errorf("failed to create grpc server: %w", err)
	}
	httpServer := ds.newHTTPServer(ctx, v1alpha1.DaemonServicePort, tlsConfig)

	conn = tls.NewListener(conn, tlsConfig)
	// Cmux is used to support servicing gRPC and HTTP1.1+JSON on the same port
	tcpm := cmux.New(conn)
	httpL := tcpm.Match(cmux.HTTP1Fast())
	grpcL := tcpm.Match(cmux.Any())

	go func() { _ = grpcServer.Serve(grpcL) }()
	go func() { _ = httpServer.Serve(httpL) }()
	go func() { _ = tcpm.Serve() }()

	log.Infof("Daemon server started successfully on %s", address)
	// Start the rater
	if err := rater.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the rater: %w", err)
	}
	<-ctx.Done()
	return nil
}

func (ds *daemonServer) newGRPCServer(
	isbSvcClient isbsvc.ISBService,
	wmFetchers map[v1alpha1.Edge][]fetch.HeadFetcher,
	rater server.Ratable) (*grpc.Server, error) {
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
	pipelineMetadataQuery, err := service.NewPipelineMetadataQuery(isbSvcClient, ds.pipeline, wmFetchers, rater)
	if err != nil {
		return nil, err
	}
	daemon.RegisterDaemonServiceServer(grpcServer, pipelineMetadataQuery)
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
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
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

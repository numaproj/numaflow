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
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/daemon/server/service"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
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
	var isbSvcClient isbsvc.ISBService
	var err error
	switch ds.isbSvcType {
	case v1alpha1.ISBSvcTypeRedis:
		isbSvcClient = isbsvc.NewISBRedisSvc(redisclient.NewInClusterRedisClient())
	case v1alpha1.ISBSvcTypeJetStream:
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(ds.pipeline.Name, isbsvc.WithJetStreamClient(jsclient.NewInClusterJetStreamClient()))
		if err != nil {
			log.Errorw("Failed to get a ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unsupported isbsvc buffer type %q", ds.isbSvcType)
	}
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
	grpcServer, err := ds.newGRPCServer(isbSvcClient)
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
	<-ctx.Done()
	return nil
}

func (ds *daemonServer) newGRPCServer(isbSvcClient isbsvc.ISBService) (*grpc.Server, error) {
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
	pipelineMetadataQuery, err := service.NewPipelineMetadataQuery(isbSvcClient, ds.pipeline)
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
	return &httpServer
}

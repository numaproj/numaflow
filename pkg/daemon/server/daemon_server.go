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
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
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
	// Start listener
	var conn net.Listener
	var listerErr error
	address := fmt.Sprintf(":%d", v1alpha1.DaemonServicePort)
	conn, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", listerErr)
	}
	var isbSvcClient isbsvc.ISBService
	switch ds.isbSvcType {
	case v1alpha1.ISBSvcTypeRedis:
		isbSvcClient = isbsvc.NewISBRedisSvc(clients.NewInClusterRedisClient())
	case v1alpha1.ISBSvcTypeJetStream:
		nc, err := clients.NewInClusterJetStreamClient().Connect(ctx)
		if err != nil {
			return fmt.Errorf("failed to get a in-cluster nats connection, %w", err)
		}
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(ds.pipeline.Name, isbsvc.WithNatsConnection(nc))
		if err != nil {
			log.Errorw("Failed to get a ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unsupported isbsvc buffer type %q", ds.isbSvcType)
	}

	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate cert: %w", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12}

	grpcServer := ds.newGRPCServer(isbSvcClient)
	httpServer := ds.newHTTPServer(ctx, v1alpha1.DaemonServicePort, tlsConfig)

	conn = tls.NewListener(conn, tlsConfig)
	// Cmux is used to support servicing gRPC and HTTP1.1+JSON on the same port
	tcpm := cmux.New(conn)
	httpL := tcpm.Match(cmux.HTTP1Fast())
	grpcL := tcpm.Match(cmux.Any())

	go func() { _ = grpcServer.Serve(grpcL) }()
	go func() { _ = httpServer.Serve(httpL) }()
	go func() { _ = tcpm.Serve() }()

	log.Infof("Daemon Server started successfully on %s", address)
	<-ctx.Done()
	return nil
}

func (ds *daemonServer) newGRPCServer(isbSvcClient isbsvc.ISBService) *grpc.Server {

	// "Prometheus histograms are a great way to measure latency distributions of your RPCs. However, since it is bad practice to have metrics of high cardinality the latency monitoring metrics are disabled by default. To enable them please call the following in your server initialization code:"
	grpc_prometheus.EnableHandlingTimeHistogram()

	sOpts := []grpc.ServerOption{
		grpc.ConnectionTimeout(300 * time.Second),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
		)),
	}
	grpcServer := grpc.NewServer(sOpts...)
	grpc_prometheus.Register(grpcServer)
	daemon.RegisterDaemonServiceServer(grpcServer, service.NewISBSvcQueryService(isbSvcClient, ds.pipeline))
	return grpcServer
}

// newHTTPServer returns the HTTP server to serve HTTP/HTTPS requests. This is implemented
// using grpc-gateway as a proxy to the gRPC server.
func (ds *daemonServer) newHTTPServer(ctx context.Context, port int, tlsConfig *tls.Config) *http.Server {
	log := logging.FromContext(ctx)
	endpoint := fmt.Sprintf("localhost:%d", port)
	mux := http.NewServeMux()
	httpServer := http.Server{
		Addr:    endpoint,
		Handler: mux,
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	}
	gwMuxOpts := runtime.WithMarshalerOption(runtime.MIMEWildcard, new(runtime.JSONPb))
	gwmux := runtime.NewServeMux(gwMuxOpts,
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) { return key, true }),
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
	)
	if err := daemon.RegisterDaemonServiceHandlerFromEndpoint(ctx, gwmux, endpoint, dialOpts); err != nil {
		log.Errorw("Failed to Register Daemon handler on HTTP Server", zap.Error(err))
	}
	return &httpServer
}

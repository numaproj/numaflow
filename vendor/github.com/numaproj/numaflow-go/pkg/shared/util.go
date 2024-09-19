package shared

import (
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/numaproj/numaflow-go/pkg/info"
)

const (
	uds = "unix"
)

func PrepareServer(sockAddr string, infoFilePath string, serverInfo *info.ServerInfo) (net.Listener, error) {
	// If serverInfo is not provided, then create a default server info instance.
	if serverInfo == nil {
		serverInfo = info.GetDefaultServerInfo()
	}
	// If infoFilePath is not empty, write the server info to the file.
	if infoFilePath != "" {
		if err := info.Write(serverInfo, info.WithServerInfoFilePath(infoFilePath)); err != nil {
			return nil, err
		}
	}

	if _, err := os.Stat(sockAddr); err == nil {
		if err := os.RemoveAll(sockAddr); err != nil {
			return nil, err
		}
	}

	lis, err := net.Listen(uds, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute net.Listen(%q, %q): %v", uds, sockAddr, err)
	}

	return lis, nil
}

func CreateGRPCServer(maxMessageSize int) *grpc.Server {
	return grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
	)
}

func StopGRPCServer(grpcServer *grpc.Server) {
	// Stop stops the gRPC server gracefully.
	// wait for the server to stop gracefully for 30 seconds
	// if it is not stopped, stop it forcefully
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	t := time.NewTimer(30 * time.Second)
	select {
	case <-t.C:
		grpcServer.Stop()
	case <-stopped:
		t.Stop()
	}
}

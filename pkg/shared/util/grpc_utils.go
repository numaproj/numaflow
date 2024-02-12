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

package util

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/numaproj/numaflow-go/pkg/info"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	resolver "github.com/numaproj/numaflow/pkg/sdkclient/grpc_resolver"
)

const defaultServerInfoFilePath = "/var/run/numaflow/server-info"

// ToUDFErr converts gRPC error to UDF Error
func ToUDFErr(name string, err error) error {
	if err == nil {
		return nil
	}
	statusCode, ok := status.FromError(err)
	// default udfError
	udfError := sdkerr.New(sdkerr.NonRetryable, statusCode.Message())
	// check if it's a standard status code
	if !ok {
		// if not, the status code will be unknown which we consider as non retryable
		// return default udfError
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
	switch statusCode.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.Unknown:
		// update to retryable err
		udfError = sdkerr.New(sdkerr.Retryable, statusCode.Message())
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	default:
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
}

// WaitForServerInfo waits until the server info is ready. It returns an error if the server info is not ready within the given timeout.
func WaitForServerInfo(timeout time.Duration, filePath string) (*info.ServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := info.WaitUntilReady(ctx, info.WithServerInfoFilePath(filePath)); err != nil {
		errMsg := fmt.Errorf("failed to wait until server info at location %q is ready: %w. Reattempting with default path %q", filePath, err, defaultServerInfoFilePath)
		log.Printf(errMsg.Error())

		// use a fall-back default server info path, in order to accommodate older sdk versions
		filePath = defaultServerInfoFilePath
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := info.WaitUntilReady(ctx, info.WithServerInfoFilePath(filePath)); err != nil {
			return nil, fmt.Errorf("failed to wait until server info at location %q is ready: %w", filePath, err)
		}
	}

	serverInfo, err := info.Read(info.WithServerInfoFilePath(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read server info: %w", err)
	}

	return serverInfo, nil
}

// ConnectToServer connects to the server with the given socket address based on the server info protocol.
func ConnectToServer(udsSockAddr string, serverInfo *info.ServerInfo, maxMessageSize int) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	var sockAddr string

	if serverInfo.Protocol == info.TCP {
		// TCP connections are used for Multiprocessing server mode, here we have multiple servers forks
		// and each server will listen on a different port.
		// On the client side we will create a connection to each of these server instances.
		// The client will use a custom resolver to resolve the server address.
		// The custom resolver will return the list of server addresses from the server info file.
		// The client will use the list of server addresses to create the multiple connections.
		if err := resolver.RegMultiProcResolver(serverInfo); err != nil {
			return nil, fmt.Errorf("failed to start Multiproc Client: %w", err)
		}

		conn, err = grpc.Dial(
			fmt.Sprintf("%s:///%s", resolver.CustScheme, resolver.CustServiceName),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		)
	} else {
		sockAddr = getUdsSockAddr(udsSockAddr)
		log.Println("UDS Client:", sockAddr)

		conn, err = grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}

	return conn, nil
}

func getUdsSockAddr(udsSock string) string {
	return fmt.Sprintf("%s:%s", "unix", udsSock)
}

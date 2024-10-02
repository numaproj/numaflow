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

package grpc

import (
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	resolver "github.com/numaproj/numaflow/pkg/sdkclient/grpc_resolver"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
)

// ConnectToServer connects to the server with the given socket address based on the server info protocol.
func ConnectToServer(udsSockAddr string, serverInfo *serverinfo.ServerInfo, maxMessageSize int) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	var sockAddr string

	// Check if Multiproc server mode is enabled
	if multiProcServer, ok := serverInfo.Metadata[serverinfo.MultiProcKey]; ok {
		// Extract the server ports from the server info file
		numServers, _ := strconv.Atoi(multiProcServer)
		// In Multiprocessing server mode we have multiple servers forks
		// and each server will listen on a different port.
		// On the client side we will create a connection to each of these server instances.
		// The client will use a custom resolver to resolve the server address.
		// The custom resolver will return the list of server addresses from the server info file.
		// The client will use the list of server addresses to create the multiple connections.
		if err := resolver.RegMultiProcResolver(numServers); err != nil {
			return nil, fmt.Errorf("failed to start Multiproc Client: %w", err)
		}

		conn, err = grpc.NewClient(
			fmt.Sprintf("%s:///%s", resolver.CustScheme, resolver.CustServiceName),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		)
	} else {
		sockAddr = getUdsSockAddr(udsSockAddr)

		conn, err = grpc.NewClient(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.NewClient(%q): %w", sockAddr, err)
	}

	return conn, nil
}

func getUdsSockAddr(udsSock string) string {
	return fmt.Sprintf("%s:%s", "unix", udsSock)
}

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

package client

import (
	"context"
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

type grpcClient struct {
	client mvtxdaemon.MonoVertexDaemonServiceClient
	conn   *grpc.ClientConn
}

var _ MonoVertexDaemonClient = (*grpcClient)(nil)

func NewGRPCClient(address string) (MonoVertexDaemonClient, error) {
	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		return nil, err
	}
	daemonClient := mvtxdaemon.NewMonoVertexDaemonServiceClient(conn)
	return &grpcClient{conn: conn, client: daemonClient}, nil
}

func (dc *grpcClient) GetMonoVertexMetrics(ctx context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	if rspn, err := dc.client.GetMonoVertexMetrics(ctx, &emptypb.Empty{}); err != nil {
		return nil, err
	} else {
		return rspn.Metrics, nil
	}
}

func (dc *grpcClient) GetMonoVertexStatus(ctx context.Context, monoVertex string) (*mvtxdaemon.MonoVertexStatus, error) {
	if rspn, err := dc.client.GetMonoVertexStatus(ctx, &mvtxdaemon.GetMonoVertexStatusRequest{Monovertex: monoVertex}); err != nil {
		return nil, err
	} else {
		return rspn.Status, nil
	}
}

// Close function closes the gRPC connection, it has to be called after a daemon client has finished all its jobs.
func (dc *grpcClient) Close() error {
	if dc.conn != nil {
		return dc.conn.Close()
	}
	return nil
}

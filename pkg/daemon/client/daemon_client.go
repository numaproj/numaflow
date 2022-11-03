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

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
)

type DaemonClient struct {
	client daemon.DaemonServiceClient
	conn   *grpc.ClientConn
}

func NewDaemonServiceClient(address string) (*DaemonClient, error) {
	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		return nil, err
	}
	daemonClient := daemon.NewDaemonServiceClient(conn)
	return &DaemonClient{conn: conn, client: daemonClient}, nil
}

// Close function closes the gRPC connection, it has to be called after a daemon client has finished all its jobs.
func (dc *DaemonClient) Close() error {
	if dc.conn != nil {
		return dc.conn.Close()
	}
	return nil
}

func (dc *DaemonClient) IsDrained(ctx context.Context, pipeline string) (bool, error) {
	rspn, err := dc.client.ListBuffers(ctx, &daemon.ListBuffersRequest{
		Pipeline: &pipeline,
	})
	if err != nil {
		return false, err
	}
	for _, bufferInfo := range rspn.Buffers {
		if *bufferInfo.PendingCount > 0 || *bufferInfo.AckPendingCount > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (dc *DaemonClient) ListPipelineBuffers(ctx context.Context, pipeline string) ([]*daemon.BufferInfo, error) {
	if rspn, err := dc.client.ListBuffers(ctx, &daemon.ListBuffersRequest{
		Pipeline: &pipeline,
	}); err != nil {
		return nil, err
	} else {
		return rspn.Buffers, nil
	}
}

func (dc *DaemonClient) GetPipelineBuffer(ctx context.Context, pipeline, buffer string) (*daemon.BufferInfo, error) {
	if rspn, err := dc.client.GetBuffer(ctx, &daemon.GetBufferRequest{
		Pipeline: &pipeline,
		Buffer:   &buffer,
	}); err != nil {
		return nil, err
	} else {
		return rspn.Buffer, nil
	}
}

func (dc *DaemonClient) GetVertexMetrics(ctx context.Context, pipeline, vertex string) (*daemon.VertexMetrics, error) {
	if rspn, err := dc.client.GetVertexMetrics(ctx, &daemon.GetVertexMetricsRequest{
		Pipeline: &pipeline,
		Vertex:   &vertex,
	}); err != nil {
		return nil, err
	} else {
		return rspn.Vertex, nil
	}
}

// GetVertexWatermark returns the VertexWatermark response instance for GetVertexWatermarkRequest
func (dc *DaemonClient) GetVertexWatermark(ctx context.Context, pipeline, vertex string) (*daemon.VertexWatermark, error) {
	if rspn, err := dc.client.GetVertexWatermark(ctx, &daemon.GetVertexWatermarkRequest{
		Pipeline: &pipeline,
		Vertex:   &vertex,
	}); err != nil {
		return nil, err
	} else {
		return rspn.VertexWatermark, nil
	}
}

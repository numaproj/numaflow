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
	return &DaemonClient{client: daemonClient}, nil
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

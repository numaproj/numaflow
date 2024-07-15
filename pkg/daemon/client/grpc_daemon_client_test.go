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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
)

type mockDaemonServiceClient struct {
	mock.Mock
}

func (m *mockDaemonServiceClient) ListBuffers(ctx context.Context, in *daemon.ListBuffersRequest, opts ...grpc.CallOption) (*daemon.ListBuffersResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*daemon.ListBuffersResponse), args.Error(1)
}

func (m *mockDaemonServiceClient) GetBuffer(ctx context.Context, in *daemon.GetBufferRequest, opts ...grpc.CallOption) (*daemon.GetBufferResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*daemon.GetBufferResponse), args.Error(1)
}

func (m *mockDaemonServiceClient) GetVertexMetrics(ctx context.Context, in *daemon.GetVertexMetricsRequest, opts ...grpc.CallOption) (*daemon.GetVertexMetricsResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*daemon.GetVertexMetricsResponse), args.Error(1)
}

func (m *mockDaemonServiceClient) GetPipelineWatermarks(ctx context.Context, in *daemon.GetPipelineWatermarksRequest, opts ...grpc.CallOption) (*daemon.GetPipelineWatermarksResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*daemon.GetPipelineWatermarksResponse), args.Error(1)
}

func (m *mockDaemonServiceClient) GetPipelineStatus(ctx context.Context, in *daemon.GetPipelineStatusRequest, opts ...grpc.CallOption) (*daemon.GetPipelineStatusResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*daemon.GetPipelineStatusResponse), args.Error(1)
}

func TestGrpcDaemonClient_ListPipelineBuffers(t *testing.T) {
	t.Run("successful listing", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedBuffers := []*daemon.BufferInfo{
			{Pipeline: "test-pipeline", BufferName: "buffer1"},
			{Pipeline: "test-pipeline", BufferName: "buffer2"},
		}

		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{Buffers: expectedBuffers}, nil)

		buffers, err := dc.ListPipelineBuffers(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Equal(t, expectedBuffers, buffers)
	})

	t.Run("empty buffer list", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{Buffers: []*daemon.BufferInfo{}}, nil)

		buffers, err := dc.ListPipelineBuffers(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Empty(t, buffers)
	})

	t.Run("error from client", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedError := errors.New("client error")
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.ListBuffersResponse)(nil), expectedError)

		buffers, err := dc.ListPipelineBuffers(context.Background(), "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, buffers)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.ListBuffersResponse)(nil), context.Canceled)

		buffers, err := dc.ListPipelineBuffers(ctx, "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, buffers)
	})
}

func TestGrpcDaemonClient_GetPipelineWatermarks(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedWatermarks := []*daemon.EdgeWatermark{
			{Pipeline: "test-pipeline", Edge: "edge1", Watermarks: []*wrapperspb.Int64Value{wrapperspb.Int64(1000), wrapperspb.Int64(2000)}},
			{Pipeline: "test-pipeline", Edge: "edge2", Watermarks: []*wrapperspb.Int64Value{wrapperspb.Int64(1000), wrapperspb.Int64(3000)}},
		}

		mockClient.On("GetPipelineWatermarks", mock.Anything, &daemon.GetPipelineWatermarksRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.GetPipelineWatermarksResponse{PipelineWatermarks: expectedWatermarks}, nil)

		watermarks, err := dc.GetPipelineWatermarks(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Equal(t, expectedWatermarks, watermarks)
	})

	t.Run("empty watermarks", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		mockClient.On("GetPipelineWatermarks", mock.Anything, &daemon.GetPipelineWatermarksRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.GetPipelineWatermarksResponse{PipelineWatermarks: []*daemon.EdgeWatermark{}}, nil)

		watermarks, err := dc.GetPipelineWatermarks(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Empty(t, watermarks)
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedError := errors.New("client error")
		mockClient.On("GetPipelineWatermarks", mock.Anything, &daemon.GetPipelineWatermarksRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.GetPipelineWatermarksResponse)(nil), expectedError)

		watermarks, err := dc.GetPipelineWatermarks(context.Background(), "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, watermarks)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockClient.On("GetPipelineWatermarks", mock.Anything, &daemon.GetPipelineWatermarksRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.GetPipelineWatermarksResponse)(nil), context.Canceled)

		watermarks, err := dc.GetPipelineWatermarks(ctx, "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, watermarks)
	})
}

func TestGrpcDaemonClient_GetPipelineBuffer(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedBuffer := &daemon.BufferInfo{
			Pipeline:        "test-pipeline",
			BufferName:      "test-buffer",
			PendingCount:    &wrapperspb.Int64Value{Value: 10},
			AckPendingCount: &wrapperspb.Int64Value{Value: 5},
		}

		mockClient.On("GetBuffer", mock.Anything, &daemon.GetBufferRequest{Pipeline: "test-pipeline", Buffer: "test-buffer"}, mock.Anything).
			Return(&daemon.GetBufferResponse{Buffer: expectedBuffer}, nil)

		buffer, err := dc.GetPipelineBuffer(context.Background(), "test-pipeline", "test-buffer")
		assert.NoError(t, err)
		assert.Equal(t, expectedBuffer, buffer)
	})

	t.Run("buffer not found", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		mockClient.On("GetBuffer", mock.Anything, &daemon.GetBufferRequest{Pipeline: "test-pipeline", Buffer: "non-existent-buffer"}, mock.Anything).
			Return((*daemon.GetBufferResponse)(nil), errors.New("buffer not found"))

		buffer, err := dc.GetPipelineBuffer(context.Background(), "test-pipeline", "non-existent-buffer")
		assert.Error(t, err)
		assert.Nil(t, buffer)
		assert.Contains(t, err.Error(), "buffer not found")
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedError := errors.New("client error")
		mockClient.On("GetBuffer", mock.Anything, &daemon.GetBufferRequest{Pipeline: "test-pipeline", Buffer: "test-buffer"}, mock.Anything).
			Return((*daemon.GetBufferResponse)(nil), expectedError)

		buffer, err := dc.GetPipelineBuffer(context.Background(), "test-pipeline", "test-buffer")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, buffer)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockClient.On("GetBuffer", mock.Anything, &daemon.GetBufferRequest{Pipeline: "test-pipeline", Buffer: "test-buffer"}, mock.Anything).
			Return((*daemon.GetBufferResponse)(nil), context.Canceled)

		buffer, err := dc.GetPipelineBuffer(ctx, "test-pipeline", "test-buffer")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, buffer)
	})
}

func TestGrpcDaemonClient_GetVertexMetrics(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedMetrics := []*daemon.VertexMetrics{
			{Vertex: "vertex1", ProcessingRates: map[string]*wrapperspb.DoubleValue{"1m": wrapperspb.Double(100)}},
			{Vertex: "vertex1", ProcessingRates: map[string]*wrapperspb.DoubleValue{"1m": wrapperspb.Double(200)}},
		}

		mockClient.On("GetVertexMetrics", mock.Anything, &daemon.GetVertexMetricsRequest{Pipeline: "test-pipeline", Vertex: "vertex1"}, mock.Anything).
			Return(&daemon.GetVertexMetricsResponse{VertexMetrics: expectedMetrics}, nil)

		metrics, err := dc.GetVertexMetrics(context.Background(), "test-pipeline", "vertex1")
		assert.NoError(t, err)
		assert.Equal(t, expectedMetrics, metrics)
	})

	t.Run("empty metrics", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		mockClient.On("GetVertexMetrics", mock.Anything, &daemon.GetVertexMetricsRequest{Pipeline: "test-pipeline", Vertex: "vertex1"}, mock.Anything).
			Return(&daemon.GetVertexMetricsResponse{VertexMetrics: []*daemon.VertexMetrics{}}, nil)

		metrics, err := dc.GetVertexMetrics(context.Background(), "test-pipeline", "vertex1")
		assert.NoError(t, err)
		assert.Empty(t, metrics)
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedError := errors.New("client error")
		mockClient.On("GetVertexMetrics", mock.Anything, &daemon.GetVertexMetricsRequest{Pipeline: "test-pipeline", Vertex: "vertex1"}, mock.Anything).
			Return((*daemon.GetVertexMetricsResponse)(nil), expectedError)

		metrics, err := dc.GetVertexMetrics(context.Background(), "test-pipeline", "vertex1")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, metrics)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockClient.On("GetVertexMetrics", mock.Anything, &daemon.GetVertexMetricsRequest{Pipeline: "test-pipeline", Vertex: "vertex1"}, mock.Anything).
			Return((*daemon.GetVertexMetricsResponse)(nil), context.Canceled)

		metrics, err := dc.GetVertexMetrics(ctx, "test-pipeline", "vertex1")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, metrics)
	})
}

func TestGrpcDaemonClient_GetPipelineStatus(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedStatus := &daemon.PipelineStatus{
			Status:  "Running",
			Message: "okay",
		}

		mockClient.On("GetPipelineStatus", mock.Anything, &daemon.GetPipelineStatusRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.GetPipelineStatusResponse{Status: expectedStatus}, nil)

		status, err := dc.GetPipelineStatus(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Equal(t, expectedStatus, status)
	})

	t.Run("pipeline not found", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		mockClient.On("GetPipelineStatus", mock.Anything, &daemon.GetPipelineStatusRequest{Pipeline: "non-existent-pipeline"}, mock.Anything).
			Return((*daemon.GetPipelineStatusResponse)(nil), errors.New("pipeline not found"))

		status, err := dc.GetPipelineStatus(context.Background(), "non-existent-pipeline")
		assert.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "pipeline not found")
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		expectedError := errors.New("client error")
		mockClient.On("GetPipelineStatus", mock.Anything, &daemon.GetPipelineStatusRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.GetPipelineStatusResponse)(nil), expectedError)

		status, err := dc.GetPipelineStatus(context.Background(), "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, status)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockClient.On("GetPipelineStatus", mock.Anything, &daemon.GetPipelineStatusRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.GetPipelineStatusResponse)(nil), context.Canceled)

		status, err := dc.GetPipelineStatus(ctx, "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, status)
	})
}
func TestGrpcDaemonClient_IsDrained(t *testing.T) {
	t.Run("all buffers empty", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{
				Buffers: []*daemon.BufferInfo{
					{PendingCount: &wrapperspb.Int64Value{Value: 0}, AckPendingCount: &wrapperspb.Int64Value{Value: 0}},
					{PendingCount: &wrapperspb.Int64Value{Value: 0}, AckPendingCount: &wrapperspb.Int64Value{Value: 0}},
				},
			}, nil)

		isDrained, err := dc.IsDrained(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.True(t, isDrained)
	})

	t.Run("one buffer with pending count", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{
				Buffers: []*daemon.BufferInfo{
					{PendingCount: &wrapperspb.Int64Value{Value: 1}, AckPendingCount: &wrapperspb.Int64Value{Value: 0}},
					{PendingCount: &wrapperspb.Int64Value{Value: 0}, AckPendingCount: &wrapperspb.Int64Value{Value: 0}},
				},
			}, nil)

		isDrained, err := dc.IsDrained(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.False(t, isDrained)
	})

	t.Run("one buffer with ack pending count", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{
				Buffers: []*daemon.BufferInfo{
					{PendingCount: &wrapperspb.Int64Value{Value: 0}, AckPendingCount: &wrapperspb.Int64Value{Value: 1}},
					{PendingCount: &wrapperspb.Int64Value{Value: 0}, AckPendingCount: &wrapperspb.Int64Value{Value: 0}},
				},
			}, nil)

		isDrained, err := dc.IsDrained(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.False(t, isDrained)
	})

	t.Run("error from ListBuffers", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}
		expectedError := errors.New("list buffers error")
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return((*daemon.ListBuffersResponse)(nil), expectedError)

		isDrained, err := dc.IsDrained(context.Background(), "test-pipeline")
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.False(t, isDrained)
	})

	t.Run("empty buffer list", func(t *testing.T) {
		mockClient := new(mockDaemonServiceClient)
		dc := &grpcDaemonClient{client: mockClient}
		mockClient.On("ListBuffers", mock.Anything, &daemon.ListBuffersRequest{Pipeline: "test-pipeline"}, mock.Anything).
			Return(&daemon.ListBuffersResponse{
				Buffers: []*daemon.BufferInfo{},
			}, nil)

		isDrained, err := dc.IsDrained(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.True(t, isDrained)
	})
}

func TestGrpcDaemonClient_Close(t *testing.T) {

	t.Run("nil connection", func(t *testing.T) {
		dc := &grpcDaemonClient{conn: nil}
		err := dc.Close()

		assert.NoError(t, err)
	})
}

func TestNewGRPCDaemonServiceClient(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		address := "localhost:8080"
		client, err := NewGRPCDaemonServiceClient(address)
		assert.NoError(t, err)
		assert.NotNil(t, client)

		grpcClient, ok := client.(*grpcDaemonClient)
		assert.True(t, ok)
		assert.NotNil(t, grpcClient.conn)
		assert.NotNil(t, grpcClient.client)

		err = client.Close()
		assert.NoError(t, err)
	})

	t.Run("empty address", func(t *testing.T) {
		address := ""
		client, err := NewGRPCDaemonServiceClient(address)
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

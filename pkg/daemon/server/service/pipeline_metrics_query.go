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

// Package service is built for querying metadata and to expose it over daemon service.
package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	rater "github.com/numaproj/numaflow/pkg/daemon/server/service/rater"
	runtimeinfo "github.com/numaproj/numaflow/pkg/daemon/server/service/runtime"
	"github.com/numaproj/numaflow/pkg/daemon/server/service/watermark"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// metricsHttpClient interface for the GET call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
}

// PipelineMetadataQuery has the metadata required for the pipeline queries
type PipelineMetadataQuery struct {
	daemon.UnimplementedDaemonServiceServer
	isbSvcClient         isbsvc.ISBService
	pipeline             *v1alpha1.Pipeline
	httpClient           metricsHttpClient
	watermarkService     *watermark.HTTPWatermarkService
	rater                rater.Ratable
	pipelineRuntimeCache runtimeinfo.PipelineRuntimeCache
	healthChecker        *HealthChecker
}

// NewPipelineMetadataQuery returns a new instance of pipelineMetadataQuery
func NewPipelineMetadataQuery(
	ctx context.Context,
	isbSvcClient isbsvc.ISBService,
	pipeline *v1alpha1.Pipeline,
	rater rater.Ratable,
	pipelineRuntimeCache runtimeinfo.PipelineRuntimeCache) (*PipelineMetadataQuery, error) {

	// Create HTTP watermark service
	watermarkService := watermark.NewHTTPWatermarkService(ctx, pipeline)

	ps := PipelineMetadataQuery{
		isbSvcClient: isbSvcClient,
		pipeline:     pipeline,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		watermarkService:     watermarkService,
		rater:                rater,
		healthChecker:        NewHealthChecker(pipeline, isbSvcClient),
		pipelineRuntimeCache: pipelineRuntimeCache,
	}
	return &ps, nil
}

// Stop stops the pipeline metadata query and cleans up resources
func (ps *PipelineMetadataQuery) Stop() {
	if ps.watermarkService != nil {
		ps.watermarkService.Stop()
	}
}

// GetPipelineWatermarks is used to return the head watermarks for a given pipeline.
func (ps *PipelineMetadataQuery) GetPipelineWatermarks(ctx context.Context, request *daemon.GetPipelineWatermarksRequest) (*daemon.GetPipelineWatermarksResponse, error) {
	return ps.watermarkService.GetPipelineWatermarks(ctx, request)
}

// ListBuffers is used to obtain the all the edge buffers information of a pipeline
func (ps *PipelineMetadataQuery) ListBuffers(ctx context.Context, req *daemon.ListBuffersRequest) (*daemon.ListBuffersResponse, error) {
	resp, err := listBuffers(ctx, ps.pipeline, ps.isbSvcClient)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetBuffer is used to obtain one buffer information of a pipeline
func (ps *PipelineMetadataQuery) GetBuffer(ctx context.Context, req *daemon.GetBufferRequest) (*daemon.GetBufferResponse, error) {
	bufferInfo, err := ps.isbSvcClient.GetBufferInfo(ctx, req.Buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to get information of buffer %q:%v", req.Buffer, err)
	}
	v := ps.pipeline.FindVertexWithBuffer(req.Buffer)
	if v == nil {
		return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", req.Buffer)
	}
	bufferLength, bufferUsageLimit := getBufferLimits(ps.pipeline, *v)
	usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
	if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
		usage = x
	}
	b := &daemon.BufferInfo{
		Pipeline:         ps.pipeline.Name,
		BufferName:       req.Buffer,
		PendingCount:     wrapperspb.Int64(bufferInfo.PendingCount),
		AckPendingCount:  wrapperspb.Int64(bufferInfo.AckPendingCount),
		TotalMessages:    wrapperspb.Int64(bufferInfo.TotalMessages),
		BufferLength:     wrapperspb.Int64(bufferLength),
		BufferUsageLimit: wrapperspb.Double(bufferUsageLimit),
		BufferUsage:      wrapperspb.Double(usage),
		IsFull:           wrapperspb.Bool(usage >= bufferUsageLimit),
	}
	resp := new(daemon.GetBufferResponse)
	resp.Buffer = b
	return resp, nil
}

// GetVertexMetrics is used to query the metrics service and is used to obtain the processing rate of a given vertex for 1m, 5m and 15m.
// Response contains the metrics for each partition of the vertex.
// In the future maybe latency will also be added here?
// Should this method live here or maybe another file?
func (ps *PipelineMetadataQuery) GetVertexMetrics(ctx context.Context, req *daemon.GetVertexMetricsRequest) (*daemon.GetVertexMetricsResponse, error) {
	resp := new(daemon.GetVertexMetricsResponse)

	pipelineName := ps.pipeline.Name
	namespace := ps.pipeline.Namespace
	vertexName := req.GetVertex()
	abstractVertex := ps.pipeline.GetVertex(vertexName)
	bufferList := abstractVertex.OwnedBufferNames(namespace, pipelineName)
	vertexType := abstractVertex.GetVertexType()

	// source vertex will have a single partition, which is the vertex name itself
	if abstractVertex.IsASource() {
		bufferList = append(bufferList, vertexName)
	}
	metricsArr := make([]*daemon.VertexMetrics, len(bufferList))

	for idx, partitionName := range bufferList {
		vm := &daemon.VertexMetrics{
			Pipeline: pipelineName,
			Vertex:   vertexName,
		}
		// get the processing rate for each partition
		vm.ProcessingRates = ps.rater.GetRates(req.GetVertex(), partitionName)
		// get the pendings for the partition
		vm.Pendings = ps.rater.GetPending(pipelineName, vertexName, string(vertexType), partitionName)
		metricsArr[idx] = vm
	}

	resp.VertexMetrics = metricsArr
	return resp, nil
}

func (ps *PipelineMetadataQuery) GetPipelineStatus(ctx context.Context, req *daemon.GetPipelineStatusRequest) (*daemon.GetPipelineStatusResponse, error) {
	status := ps.healthChecker.getCurrentHealth()
	resp := new(daemon.GetPipelineStatusResponse)
	resp.Status = &daemon.PipelineStatus{
		Status:  status.Status,
		Message: status.Message,
		Code:    status.Code,
	}
	return resp, nil
}

// GetVertexErrors returns errors for a given vertex by accessing the local cache in the runtime service.
// The errors are persisted in the local cache by the runtime service.
// Errors are retrieved for all active replicas for a given vertex.
// A list of replica errors for a given vertex is returned.
func (ps *PipelineMetadataQuery) GetVertexErrors(ctx context.Context, req *daemon.GetVertexErrorsRequest) (*daemon.GetVertexErrorsResponse, error) {
	pipeline, vertex := req.GetPipeline(), req.GetVertex()
	cacheKey := fmt.Sprintf("%s-%s", pipeline, vertex)
	resp := new(daemon.GetVertexErrorsResponse)
	localCache := ps.pipelineRuntimeCache.GetLocalCache()

	// If the errors are present in the local cache, return the errors.
	if errors, ok := localCache[cacheKey]; ok {
		replicaErrors := make([]*daemon.ReplicaErrors, len(errors))
		for i, err := range errors {
			containerErrors := make([]*daemon.ContainerError, len(err.ContainerErrors))
			for j, containerError := range err.ContainerErrors {
				containerErrors[j] = &daemon.ContainerError{
					Container: containerError.Container,
					Timestamp: timestamppb.New(time.Unix(containerError.Timestamp, 0)),
					Code:      containerError.Code,
					Message:   containerError.Message,
					Details:   containerError.Details,
				}
			}
			replicaErrors[i] = &daemon.ReplicaErrors{
				Replica:         err.Replica,
				ContainerErrors: containerErrors,
			}
		}
		resp.Errors = replicaErrors
	}

	return resp, nil
}

func getBufferLimits(pl *v1alpha1.Pipeline, v v1alpha1.AbstractVertex) (bufferLength int64, bufferUsageLimit float64) {
	plLimits := pl.GetPipelineLimits()
	bufferLength = int64(*plLimits.BufferMaxLength)
	bufferUsageLimit = float64(*plLimits.BufferUsageLimit) / 100
	if x := v.Limits; x != nil {
		if x.BufferMaxLength != nil {
			bufferLength = int64(*x.BufferMaxLength)
		}
		if x.BufferUsageLimit != nil {
			bufferUsageLimit = float64(*x.BufferUsageLimit) / 100
		}
	}
	return bufferLength, bufferUsageLimit
}

// listBuffers returns the list of ISB buffers for the pipeline and their information
// We use the isbSvcClient to get the buffer information
func listBuffers(ctx context.Context, pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService) (*daemon.ListBuffersResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.ListBuffersResponse)

	buffers := []*daemon.BufferInfo{}
	for _, buffer := range pipeline.GetAllBuffers() {
		bufferInfo, err := isbSvcClient.GetBufferInfo(ctx, buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to get information of buffer %q", buffer)
		}
		log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
		v := pipeline.FindVertexWithBuffer(buffer)
		if v == nil {
			return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", buffer)
		}
		bufferLength, bufferUsageLimit := getBufferLimits(pipeline, *v)
		usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
		if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
			usage = x
		}
		b := &daemon.BufferInfo{
			Pipeline:         pipeline.Name,
			BufferName:       buffer,
			PendingCount:     wrapperspb.Int64(bufferInfo.PendingCount),
			AckPendingCount:  wrapperspb.Int64(bufferInfo.AckPendingCount),
			TotalMessages:    wrapperspb.Int64(bufferInfo.TotalMessages),
			BufferLength:     wrapperspb.Int64(bufferLength),
			BufferUsageLimit: wrapperspb.Double(bufferUsageLimit),
			BufferUsage:      wrapperspb.Double(usage),
			IsFull:           wrapperspb.Bool(usage >= bufferUsageLimit),
		}
		buffers = append(buffers, b)
	}
	resp.Buffers = buffers
	return resp, nil
}

// StartHealthCheck starts the health check for the pipeline using the health checker
func (ps *PipelineMetadataQuery) StartHealthCheck(ctx context.Context) {
	ps.healthChecker.startHealthCheck(ctx)
}

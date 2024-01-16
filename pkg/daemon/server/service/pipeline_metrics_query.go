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

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	rater "github.com/numaproj/numaflow/pkg/daemon/server/service/rater"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
)

// metricsHttpClient interface for the GET call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
}

// PipelineMetadataQuery has the metadata required for the pipeline queries
type PipelineMetadataQuery struct {
	isbSvcClient      isbsvc.ISBService
	pipeline          *v1alpha1.Pipeline
	httpClient        metricsHttpClient
	watermarkFetchers map[v1alpha1.Edge][]fetch.HeadFetcher
	rater             rater.Ratable
	healthChecker     *HealthChecker
}

const (
	PipelineStatusHealthy  = "healthy"
	PipelineStatusError    = "Error"
	PipelineStatusUnknown  = "Unknown"
	PipelineStatusCritical = "critical"
	PipelineStatusWarning  = "warning"
	PipelineStatusInactive = "inactive"
	PipelineStatusDeleting = "deleting"
)

// NewPipelineMetadataQuery returns a new instance of pipelineMetadataQuery
func NewPipelineMetadataQuery(
	isbSvcClient isbsvc.ISBService,
	pipeline *v1alpha1.Pipeline,
	wmFetchers map[v1alpha1.Edge][]fetch.HeadFetcher,
	rater rater.Ratable) (*PipelineMetadataQuery, error) {
	var err error
	ps := PipelineMetadataQuery{
		isbSvcClient: isbSvcClient,
		pipeline:     pipeline,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		watermarkFetchers: wmFetchers,
		rater:             rater,
		healthChecker:     NewHealthChecker(pipeline, isbSvcClient),
	}
	if err != nil {
		return nil, err
	}
	return &ps, nil
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
	bufferInfo, err := ps.isbSvcClient.GetBufferInfo(ctx, *req.Buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to get information of buffer %q:%v", *req.Buffer, err)
	}
	v := ps.pipeline.FindVertexWithBuffer(*req.Buffer)
	if v == nil {
		return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", *req.Buffer)
	}
	bufferLength, bufferUsageLimit := getBufferLimits(ps.pipeline, *v)
	usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
	if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
		usage = x
	}
	b := &daemon.BufferInfo{
		Pipeline:         &ps.pipeline.Name,
		BufferName:       req.Buffer,
		PendingCount:     &bufferInfo.PendingCount,
		AckPendingCount:  &bufferInfo.AckPendingCount,
		TotalMessages:    &bufferInfo.TotalMessages,
		BufferLength:     &bufferLength,
		BufferUsageLimit: &bufferUsageLimit,
		BufferUsage:      &usage,
		IsFull:           pointer.Bool(usage >= bufferUsageLimit),
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

	abstractVertex := ps.pipeline.GetVertex(req.GetVertex())
	bufferList := abstractVertex.OwnedBufferNames(ps.pipeline.Namespace, ps.pipeline.Name)

	// source vertex will have a single partition, which is the vertex name itself
	if abstractVertex.IsASource() {
		bufferList = append(bufferList, req.GetVertex())
	}
	partitionPendingInfo := ps.getPending(ctx, req)
	metricsArr := make([]*daemon.VertexMetrics, len(bufferList))

	for idx, partitionName := range bufferList {
		vm := &daemon.VertexMetrics{
			Pipeline: &ps.pipeline.Name,
			Vertex:   req.Vertex,
		}
		// get the processing rate for each partition
		vm.ProcessingRates = ps.rater.GetRates(req.GetVertex(), partitionName)
		vm.Pendings = partitionPendingInfo[partitionName]
		metricsArr[idx] = vm
	}

	resp.VertexMetrics = metricsArr
	return resp, nil
}

// getPending returns the pending count for each partition of the vertex
func (ps *PipelineMetadataQuery) getPending(ctx context.Context, req *daemon.GetVertexMetricsRequest) map[string]map[string]int64 {
	vertexName := fmt.Sprintf("%s-%s", ps.pipeline.Name, req.GetVertex())
	log := logging.FromContext(ctx)

	vertex := &v1alpha1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Name: vertexName,
		},
	}
	abstractVertex := ps.pipeline.GetVertex(req.GetVertex())

	metricsCount := 1
	if abstractVertex.IsReduceUDF() {
		metricsCount = abstractVertex.GetPartitionCount()
	}
	headlessServiceName := vertex.GetHeadlessServiceName()
	totalPendingMap := make(map[string]map[string]int64)
	for idx := 0; idx < metricsCount; idx++ {
		// Get the headless service name
		// We can query the metrics endpoint of the (i)th pod to obtain this value.
		// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc:2469/metrics
		url := fmt.Sprintf("https://%s-%v.%s.%s.svc:%v/metrics", vertexName, idx, headlessServiceName, ps.pipeline.Namespace, v1alpha1.VertexMetricsPort)
		if res, err := ps.httpClient.Get(url); err != nil {
			log.Debugf("Error reading the metrics endpoint, it might be because of vertex scaling down to 0: %f", err.Error())
			return nil
		} else {
			// expfmt Parser from prometheus to parse the metrics
			textParser := expfmt.TextParser{}
			result, err := textParser.TextToMetricFamilies(res.Body)
			if err != nil {
				log.Errorw("Error in parsing to prometheus metric families", zap.Error(err))
				return nil
			}

			// Get the pending messages for this partition
			if value, ok := result[metrics.VertexPendingMessages]; ok {
				metricsList := value.GetMetric()
				for _, metric := range metricsList {
					labels := metric.GetLabel()
					lookback := ""
					partitionName := ""
					for _, label := range labels {
						if label.GetName() == metrics.LabelPeriod {
							lookback = label.GetValue()

						}
						if label.GetName() == metrics.LabelPartitionName {
							partitionName = label.GetValue()
						}
					}
					if _, ok := totalPendingMap[partitionName]; !ok {
						totalPendingMap[partitionName] = make(map[string]int64)
					}
					totalPendingMap[partitionName][lookback] += int64(metric.Gauge.GetValue())
				}
			}
		}
	}
	return totalPendingMap
}

func (ps *PipelineMetadataQuery) GetPipelineStatus(ctx context.Context, req *daemon.GetPipelineStatusRequest) (*daemon.GetPipelineStatusResponse, error) {
	status := ps.healthChecker.getCurrentHealth()
	resp := new(daemon.GetPipelineStatusResponse)
	resp.Status = &daemon.PipelineStatus{
		Status:  pointer.String(status.Status),
		Message: pointer.String(status.Message),
		Code:    pointer.String(status.Code),
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
			Pipeline:         &pipeline.Name,
			BufferName:       pointer.String(fmt.Sprintf("%v", buffer)),
			PendingCount:     &bufferInfo.PendingCount,
			AckPendingCount:  &bufferInfo.AckPendingCount,
			TotalMessages:    &bufferInfo.TotalMessages,
			BufferLength:     &bufferLength,
			BufferUsageLimit: &bufferUsageLimit,
			BufferUsage:      &usage,
			IsFull:           pointer.Bool(usage >= bufferUsageLimit),
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

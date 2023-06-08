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

	server "github.com/numaproj/numaflow/pkg/daemon/server/service/rater"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
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

// pipelineMetadataQuery has the metadata required for the pipeline queries
type pipelineMetadataQuery struct {
	isbSvcClient      isbsvc.ISBService
	pipeline          *v1alpha1.Pipeline
	httpClient        metricsHttpClient
	watermarkFetchers map[string][]fetch.Fetcher
	rater             server.Ratable
}

const (
	PipelineStatusOK      = "OK"
	PipelineStatusError   = "Error"
	PipelineStatusUnknown = "Unknown"
)

// NewPipelineMetadataQuery returns a new instance of pipelineMetadataQuery
func NewPipelineMetadataQuery(
	isbSvcClient isbsvc.ISBService,
	pipeline *v1alpha1.Pipeline,
	wmFetchers map[string][]fetch.Fetcher,
	rater server.Ratable) (*pipelineMetadataQuery, error) {
	var err error
	ps := pipelineMetadataQuery{
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
	}
	if err != nil {
		return nil, err
	}
	return &ps, nil
}

// ListBuffers is used to obtain the all the edge buffers information of a pipeline
func (ps *pipelineMetadataQuery) ListBuffers(ctx context.Context, req *daemon.ListBuffersRequest) (*daemon.ListBuffersResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.ListBuffersResponse)

	buffers := []*daemon.BufferInfo{}
	for _, buffer := range ps.pipeline.GetAllBuffers() {
		bufferInfo, err := ps.isbSvcClient.GetBufferInfo(ctx, buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to get information of buffer %q", buffer)
		}
		log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
		v := ps.pipeline.FindVertexWithBuffer(buffer)
		if v == nil {
			return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", buffer)
		}
		bufferLength, bufferUsageLimit := getBufferLimits(ps.pipeline, *v)
		usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
		if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
			usage = x
		}
		b := &daemon.BufferInfo{
			Pipeline:         &ps.pipeline.Name,
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

// GetBuffer is used to obtain one buffer information of a pipeline
func (ps *pipelineMetadataQuery) GetBuffer(ctx context.Context, req *daemon.GetBufferRequest) (*daemon.GetBufferResponse, error) {
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
// In the future maybe latency will also be added here?
// Should this method live here or maybe another file?
func (ps *pipelineMetadataQuery) GetVertexMetrics(ctx context.Context, req *daemon.GetVertexMetricsRequest) (*daemon.GetVertexMetricsResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.GetVertexMetricsResponse)
	vertexName := fmt.Sprintf("%s-%s", ps.pipeline.Name, req.GetVertex())

	// Get the headless service name
	vertex := &v1alpha1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Name: vertexName,
		},
	}
	headlessServiceName := vertex.GetHeadlessServiceName()

	abstractVertex := ps.pipeline.GetVertex(req.GetVertex())
	vertexLevelRates := ps.rater.GetRates(req.GetVertex())

	metricsArr := make([]*daemon.VertexMetrics, abstractVertex.GetPartitionCount())
	for i := int64(0); i < int64(abstractVertex.GetPartitionCount()); i++ {
		// We can query the metrics endpoint of the (i)th pod to obtain this value.
		// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
		url := fmt.Sprintf("https://%s-%v.%s.%s.svc.cluster.local:%v/metrics", vertexName, i, headlessServiceName, ps.pipeline.Namespace, v1alpha1.VertexMetricsPort)
		if res, err := ps.httpClient.Get(url); err != nil {
			log.Debugf("Error reading the metrics endpoint, it might be because of vertex scaling down to 0: %f", err.Error())
			metricsArr[i] = &daemon.VertexMetrics{
				Pipeline: &ps.pipeline.Name,
				Vertex:   req.Vertex,
			}
		} else {
			// expfmt Parser from prometheus to parse the metrics
			textParser := expfmt.TextParser{}
			result, err := textParser.TextToMetricFamilies(res.Body)
			if err != nil {
				log.Errorw("Error in parsing to prometheus metric families", zap.Error(err))
				return nil, err
			}

			// Get the pending messages for this partition
			pendings := make(map[string]int64, 0)
			if value, ok := result[metrics.VertexPendingMessages]; ok {
				metricsList := value.GetMetric()
				for _, metric := range metricsList {
					labels := metric.GetLabel()
					for _, label := range labels {
						if label.GetName() == metrics.LabelPeriod {
							lookback := label.GetValue()
							pendings[lookback] = int64(metric.Gauge.GetValue())
						}
					}
				}
			}
			vm := &daemon.VertexMetrics{
				Pipeline: &ps.pipeline.Name,
				Vertex:   req.Vertex,
				Pendings: pendings,
			}

			// Get the processing rate for this partition
			if abstractVertex.IsReduceUDF() {
				// the processing rate of this ith partition is the rate of the corresponding ith pod.
				vm.ProcessingRates = ps.rater.GetPodRates(req.GetVertex(), int(i))
			} else {
				// if the vertex is not a reduce udf, then the processing rate is the sum of all pods in this vertex.
				// TODO (multi-partition) - change this to display the processing rate of each partition when we finish multi-partition support for non-reduce vertices.
				vm.ProcessingRates = vertexLevelRates
			}

			metricsArr[i] = vm
		}
	}

	resp.VertexMetrics = metricsArr
	return resp, nil
}

func (ps *pipelineMetadataQuery) GetPipelineStatus(ctx context.Context, req *daemon.GetPipelineStatusRequest) (*daemon.GetPipelineStatusResponse, error) {

	resp := new(daemon.GetPipelineStatusResponse)

	// get all vertices of pipeline
	vertices := ps.pipeline.Spec.Vertices

	// loop over vertices and get metrics to check pending messages vs processing rate
	for _, vertex := range vertices {
		vertexReq := new(daemon.GetVertexMetricsRequest)
		vertexReq.Vertex = &vertex.Name
		vertexResp, err := ps.GetVertexMetrics(ctx, vertexReq)
		// if err is not nil, more than likely autoscaling is down to 0 and metrics are not available
		if err != nil {
			resp.Status = &daemon.PipelineStatus{
				Status:  pointer.String(PipelineStatusUnknown),
				Message: pointer.String("Pipeline status is unknown."),
			}
			return resp, nil
		}

		// may need to revisit later, another concern could be that the processing rate is too slow instead of just 0
		for _, vertexMetrics := range vertexResp.VertexMetrics {
			var pending int64
			var processingRate float64
			if p, ok := vertexMetrics.GetPendings()["default"]; ok {
				pending = p
			} else {
				continue
			}

			if p, ok := vertexMetrics.GetProcessingRates()["default"]; ok {
				processingRate = p
			} else {
				continue
			}

			if pending > 0 && processingRate == 0 {
				resp.Status = &daemon.PipelineStatus{
					Status:  pointer.String(PipelineStatusError),
					Message: pointer.String(fmt.Sprintf("Pipeline has an error. Vertex %s is not processing pending messages.", vertex.Name)),
				}
				return resp, nil
			}
		}
	}

	resp.Status = &daemon.PipelineStatus{
		Status:  pointer.String(PipelineStatusOK),
		Message: pointer.String("Pipeline has no issue."),
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

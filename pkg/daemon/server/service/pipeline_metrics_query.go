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
	"github.com/numaproj/numaflow/pkg/isbsvc"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// metricsHttpClient interface for the GET call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
}

// pipelineMetadataQuery has the metadata required for the pipeline queries
type pipelineMetadataQuery struct {
	isbSvcClient    isbsvc.ISBService
	pipeline        *v1alpha1.Pipeline
	httpClient      metricsHttpClient
	vertexWatermark *watermarkFetchers
}

// NewPipelineMetadataQuery returns a new instance of pipelineMetadataQuery
func NewPipelineMetadataQuery(isbSvcClient isbsvc.ISBService, pipeline *v1alpha1.Pipeline) (*pipelineMetadataQuery, error) {
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
	}
	ps.vertexWatermark, err = newVertexWatermarkFetcher(pipeline, isbSvcClient)
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
	for _, edge := range ps.pipeline.ListAllEdges() {
		bs := v1alpha1.GenerateEdgeBufferNames(ps.pipeline.Namespace, ps.pipeline.Name, edge)
		for _, buffer := range bs {
			bufferInfo, err := ps.isbSvcClient.GetBufferInfo(ctx, v1alpha1.Buffer{Name: buffer, Type: v1alpha1.EdgeBuffer})
			if err != nil {
				return nil, fmt.Errorf("failed to get information of buffer %q", buffer)
			}
			log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
			bufferLength, bufferUsageLimit := getBufferLimits(ps.pipeline, edge)
			usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
			if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
				usage = x
			}
			b := &daemon.BufferInfo{
				Pipeline:         &ps.pipeline.Name,
				FromVertex:       pointer.String(fmt.Sprintf("%v", edge.From)),
				ToVertex:         pointer.String(fmt.Sprintf("%v", edge.To)),
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
	}
	resp.Buffers = buffers
	return resp, nil
}

// GetBuffer is used to obtain one buffer information of a pipeline
func (ps *pipelineMetadataQuery) GetBuffer(ctx context.Context, req *daemon.GetBufferRequest) (*daemon.GetBufferResponse, error) {
	bufferInfo, err := ps.isbSvcClient.GetBufferInfo(ctx, v1alpha1.Buffer{Name: *req.Buffer, Type: v1alpha1.EdgeBuffer})
	if err != nil {
		return nil, fmt.Errorf("failed to get information of buffer %q:%v", *req.Buffer, err)
	}
	edge := ps.pipeline.FindEdgeWithBuffer(*req.Buffer)
	if edge == nil {
		return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", *req.Buffer)
	}
	bufferLength, bufferUsageLimit := getBufferLimits(ps.pipeline, *edge)
	usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
	if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
		usage = x
	}
	b := &daemon.BufferInfo{
		Pipeline:         &ps.pipeline.Name,
		FromVertex:       &edge.From,
		ToVertex:         &edge.To,
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
	vertex := &v1alpha1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Name: vertexName,
		},
	}

	processingRates := make(map[string]float64, 0)
	pendings := make(map[string]int64, 0)

	// Get the headless service name
	headlessServiceName := vertex.GetHeadlessServiceName()
	// We can query the metrics endpoint of the 0th pod to obtain this value.
	// example: https://simple-pipeline-in-0.simple-pipeline-in-headless.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s-0.%s.%s.svc.cluster.local:%v/metrics", vertexName, headlessServiceName, ps.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := ps.httpClient.Get(url); err != nil {
		log.Debugf("Error reading the metrics endpoint, it might be because of vertex scaling down to 0: %f", err.Error())
	} else {
		// expfmt Parser from prometheus to parse the metrics
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			log.Errorw("Error in parsing to prometheus metric families", zap.Error(err))
			return nil, err
		}

		// Check if the resultant metrics list contains the processingRate, if it does look for the period label
		if value, ok := result[metricspkg.VertexProcessingRate]; ok {
			metrics := value.GetMetric()
			for _, metric := range metrics {
				labels := metric.GetLabel()
				for _, label := range labels {
					if label.GetName() == metricspkg.LabelPeriod {
						lookback := label.GetValue()
						processingRates[lookback] = metric.Gauge.GetValue()
					}
				}
			}
		}

		if value, ok := result[metricspkg.VertexPendingMessages]; ok {
			metrics := value.GetMetric()
			for _, metric := range metrics {
				labels := metric.GetLabel()
				for _, label := range labels {
					if label.GetName() == metricspkg.LabelPeriod {
						lookback := label.GetValue()
						pendings[lookback] = int64(metric.Gauge.GetValue())
					}
				}
			}
		}
	}

	v := &daemon.VertexMetrics{
		Pipeline:        &ps.pipeline.Name,
		Vertex:          req.Vertex,
		ProcessingRates: processingRates,
		Pendings:        pendings,
	}
	resp.Vertex = v
	return resp, nil
}

func getBufferLimits(pl *v1alpha1.Pipeline, edge v1alpha1.Edge) (bufferLength int64, bufferUsageLimit float64) {
	plLimits := pl.GetPipelineLimits()
	bufferLength = int64(*plLimits.BufferMaxLength)
	bufferUsageLimit = float64(*plLimits.BufferUsageLimit) / 100
	if x := edge.Limits; x != nil {
		if x.BufferMaxLength != nil {
			bufferLength = int64(*x.BufferMaxLength)
		}
		if x.BufferUsageLimit != nil {
			bufferUsageLimit = float64(*x.BufferUsageLimit) / 100
		}
	}
	return bufferLength, bufferUsageLimit
}

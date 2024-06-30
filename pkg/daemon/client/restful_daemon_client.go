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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
)

type restfulDaemonClient struct {
	hostURL    string
	httpClient *http.Client
}

var _ DaemonClient = (*restfulDaemonClient)(nil)

func NewRESTfulDaemonServiceClient(address string) (DaemonClient, error) {
	if !strings.HasPrefix(address, "https://") {
		address = "https://" + address
	}
	client := &restfulDaemonClient{
		hostURL: address,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
	}
	return client, nil
}

func (rc *restfulDaemonClient) Close() error {
	return nil
}

func unmarshalResponse[T any](r *http.Response) (*T, error) {
	if r.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected response %v: %s", r.StatusCode, r.Status)
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read data from response body, %w", err)
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body to %T, %w", t, err)
	}
	return &t, nil
}

func (rc *restfulDaemonClient) IsDrained(ctx context.Context, pipeline string) (bool, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/buffers", rc.hostURL, pipeline))
	if err != nil {
		return false, fmt.Errorf("failed to call list buffers RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	res, err := unmarshalResponse[daemon.ListBuffersResponse](resp)
	if err != nil {
		return false, err
	}
	for _, bufferInfo := range res.Buffers {
		if bufferInfo.PendingCount.GetValue() > 0 || bufferInfo.AckPendingCount.GetValue() > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (rc *restfulDaemonClient) ListPipelineBuffers(ctx context.Context, pipeline string) ([]*daemon.BufferInfo, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/buffers", rc.hostURL, pipeline))
	if err != nil {
		return nil, fmt.Errorf("failed to call list buffers RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[daemon.ListBuffersResponse](resp); err != nil {
		return nil, err
	} else {
		return res.Buffers, nil
	}
}

func (rc *restfulDaemonClient) GetPipelineBuffer(ctx context.Context, pipeline, buffer string) (*daemon.BufferInfo, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/buffers/%s", rc.hostURL, pipeline, buffer))
	if err != nil {
		return nil, fmt.Errorf("failed to call get buffer RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[daemon.GetBufferResponse](resp); err != nil {
		return nil, err
	} else {
		return res.Buffer, nil
	}
}

func (rc *restfulDaemonClient) GetVertexMetrics(ctx context.Context, pipeline, vertex string) ([]*daemon.VertexMetrics, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/vertices/%s/metrics", rc.hostURL, pipeline, vertex))
	if err != nil {
		return nil, fmt.Errorf("failed to call get vertex metrics RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[daemon.GetVertexMetricsResponse](resp); err != nil {
		return nil, err
	} else {
		return res.VertexMetrics, nil
	}
}

// GetPipelineWatermarks returns the []EdgeWatermark response instance for GetPipelineWatermarksRequest
func (rc *restfulDaemonClient) GetPipelineWatermarks(ctx context.Context, pipeline string) ([]*daemon.EdgeWatermark, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/watermarks", rc.hostURL, pipeline))
	if err != nil {
		return nil, fmt.Errorf("failed to call get pipeline watermark RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[daemon.GetPipelineWatermarksResponse](resp); err != nil {
		return nil, err
	} else {
		return res.PipelineWatermarks, nil
	}
}

func (rc *restfulDaemonClient) GetPipelineStatus(ctx context.Context, pipeline string) (*daemon.PipelineStatus, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/pipelines/%s/status", rc.hostURL, pipeline))
	if err != nil {
		return nil, fmt.Errorf("failed to call get pipeline status RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[daemon.GetPipelineStatusResponse](resp); err != nil {
		return nil, err
	} else {
		return res.Status, nil
	}
}

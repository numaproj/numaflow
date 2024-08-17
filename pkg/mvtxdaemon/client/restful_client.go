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
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

var (
	// Use JSONPb to unmarshal the response, it is needed to unmarshal the response with google.protobuf.* data types.
	jsonMarshaller = new(runtime.JSONPb)
)

type restfulClient struct {
	hostURL    string
	httpClient *http.Client
}

var _ MonoVertexDaemonClient = (*restfulClient)(nil)

func NewRESTfulClient(address string) (MonoVertexDaemonClient, error) {
	if !strings.HasPrefix(address, "https://") {
		address = "https://" + address
	}
	client := &restfulClient{
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

func (rc *restfulClient) Close() error {
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
	if err := jsonMarshaller.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body to %T, %w", t, err)
	}
	return &t, nil
}

func (rc *restfulClient) GetMonoVertexMetrics(ctx context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	resp, err := rc.httpClient.Get(fmt.Sprintf("%s/api/v1/metrics", rc.hostURL))
	if err != nil {
		return nil, fmt.Errorf("failed to call get mono vertex metrics RESTful API, %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if res, err := unmarshalResponse[mvtxdaemon.GetMonoVertexMetricsResponse](resp); err != nil {
		return nil, err
	} else {
		return res.Metrics, nil
	}
}

func (rc *restfulClient) GetMonoVertexStatus(ctx context.Context, monoVertex string) (*mvtxdaemon.GetMonoVertexStatusResponse, error) {
	//TODO implement me
	panic("implement me")
}

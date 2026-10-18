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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRestfulClient_GetMonoVertexPodReadRates(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/pods/read-rates", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"pods":[{"hostname":"simple-mv-0","replica":0,"readRates":{"15m":0}}]}`))
	}))
	defer server.Close()

	daemonClient, err := NewRESTfulClient(server.URL)
	assert.NoError(t, err)
	pods, err := daemonClient.GetMonoVertexPodReadRates(context.Background())
	assert.NoError(t, err)
	if assert.Len(t, pods, 1) {
		assert.Equal(t, "simple-mv-0", pods[0].Hostname)
		assert.Equal(t, 0.0, pods[0].ReadRates["15m"].GetValue())
	}
}

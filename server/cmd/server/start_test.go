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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/numaproj/numaflow/server/authz"
)

func TestCreateAuthRouteMap(t *testing.T) {
	t.Run("empty base", func(t *testing.T) {
		got := CreateAuthRouteMap("")
		assert.Equal(t, 44, len(got))
		assert.Contains(t, got, "GET:api/v1/namespaces/:namespace/isb-services/:isb-service/jetstream")
		assert.Contains(t, got, "GET:api/v1/namespaces/:namespace/pipelines/:pipeline/isb/streams")
		assert.Contains(t, got, "GET:api/v1/namespaces/:namespace/pipelines/:pipeline/isb/consumers")
		assert.Contains(t, got, "GET:api/v1/namespaces/:namespace/pipelines/:pipeline/isb/kv-stores")
		assert.Contains(t, got, "GET:api/v2/capabilities")
		assert.Contains(t, got, "GET:api/v2/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/summary")

		routeObjects := map[string]string{
			"GET:api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods/:pod/logs": authz.ObjectPipeline,
			"GET:api/v1/namespaces/:namespace/mono-vertices/:mono-vertex/pods/:pod/logs":           authz.ObjectMonoVertex,
			"GET:api/v1/namespaces/:namespace/pods/:pod/logs":                                      authz.ObjectAll,
		}
		for route, expectedObject := range routeObjects {
			routeInfo, ok := got[route]
			require.True(t, ok, "route %q is missing", route)
			assert.Equal(t, expectedObject, routeInfo.Object)
		}
	})

	t.Run("customize base", func(t *testing.T) {
		got := CreateAuthRouteMap("abcdefg")
		assert.Equal(t, 44, len(got))
		for k := range got {
			assert.Contains(t, k, "abcdefg")
		}
	})
}

func TestNewServer(t *testing.T) {
	opts := ServerOptions{
		Insecure:             true,
		Port:                 8080,
		Namespaced:           true,
		ManagedNamespace:     "default",
		BaseHref:             "/",
		DisableAuth:          false,
		DexServerAddr:        "http://dex:5556",
		ServerAddr:           "http://server:8080",
		CorsAllowedOrigins:   "http://localhost:3000,http://example.com",
		ReadOnly:             false,
		DaemonClientProtocol: "http",
	}

	s := NewServer(opts)

	assert.NotNil(t, s)
	assert.Equal(t, opts, s.options)
}

func TestNeedToRewrite(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "path with rewrite prefix",
			path:     "/namespaces/abc",
			expected: true,
		},
		{
			name:     "path without rewrite prefix",
			path:     "/static/images/logo.png",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := needToRewrite(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the License for the specific language governing permissions and
limitations under the License.
*/

package mcp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClusterSvcAddr(t *testing.T) {
	svc, ns, port, err := parseClusterSvcAddr("simple-pipeline-daemon-svc.default.svc:4327")
	require.NoError(t, err)
	assert.Equal(t, "simple-pipeline-daemon-svc", svc)
	assert.Equal(t, "default", ns)
	assert.Equal(t, 4327, port)

	svc, ns, port, err = parseClusterSvcAddr("map-mono-vertex-mv-daemon-svc.numaflow-demo.svc:4327")
	require.NoError(t, err)
	assert.Equal(t, "map-mono-vertex-mv-daemon-svc", svc)
	assert.Equal(t, "numaflow-demo", ns)
	assert.Equal(t, 4327, port)

	_, _, _, err = parseClusterSvcAddr("invalid")
	assert.Error(t, err)
}

func TestDaemonConnectorResolveInCluster(t *testing.T) {
	// nil connector returns address unchanged.
	var c *daemonConnector
	addr, err := c.resolve(t.Context(), "simple-pipeline-daemon-svc.default.svc:4327")
	require.NoError(t, err)
	assert.Equal(t, "simple-pipeline-daemon-svc.default.svc:4327", addr)
}

//go:build integration

/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the License specific language governing permissions and
limitations under the License.
*/

package mcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

func TestDaemonConnectorPortForwardMapMonoVertex(t *testing.T) {
	if isInCluster() {
		t.Skip("integration test targets out-of-cluster port-forward path")
	}
	restConfig, err := util.K8sRestConfig()
	require.NoError(t, err)
	kube, _, err := NewClients()
	require.NoError(t, err)

	connector := newDaemonConnector(restConfig, kube)
	addr, err := connector.resolve(context.Background(), "map-mono-vertex-mv-daemon-svc.numaflow-demo.svc:4327")
	require.NoError(t, err)
	require.Contains(t, addr, "127.0.0.1:")

	client, err := mvtdaemonclient.NewGRPCClient(addr)
	require.NoError(t, err)
	defer client.Close()

	metrics, err := client.GetMonoVertexMetrics(context.Background())
	require.NoError(t, err)
	require.NotNil(t, metrics)
	require.Equal(t, "map-mono-vertex", metrics.MonoVertex)

	errs, err := client.GetMonoVertexErrors(context.Background(), "map-mono-vertex")
	require.NoError(t, err)
	t.Logf("map-mono-vertex metrics: %+v", metrics)
	t.Logf("map-mono-vertex errors: %+v", errs)
}

func TestDaemonConnectorPortForwardMinimalMonoVertex(t *testing.T) {
	if isInCluster() {
		t.Skip("integration test targets out-of-cluster port-forward path")
	}
	restConfig, err := util.K8sRestConfig()
	require.NoError(t, err)
	kube, _, err := NewClients()
	require.NoError(t, err)

	connector := newDaemonConnector(restConfig, kube)
	addr, err := connector.resolve(context.Background(), "minimal-mono-vertex-mv-daemon-svc.numaflow-demo.svc:4327")
	require.NoError(t, err)
	require.Contains(t, addr, "127.0.0.1:")

	client, err := mvtdaemonclient.NewGRPCClient(addr)
	require.NoError(t, err)
	defer client.Close()

	metrics, err := client.GetMonoVertexMetrics(context.Background())
	require.NoError(t, err)
	require.NotNil(t, metrics)
	require.Equal(t, "minimal-mono-vertex", metrics.MonoVertex)

	errs, err := client.GetMonoVertexErrors(context.Background(), "minimal-mono-vertex")
	require.NoError(t, err)
	t.Logf("minimal-mono-vertex metrics: %+v", metrics)
	t.Logf("minimal-mono-vertex errors: %+v", errs)
}

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

package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	dffake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeDaemonClient implements DaemonClient for testing.
type fakeDaemonClient struct {
	status     *daemon.PipelineStatus
	watermarks []*daemon.EdgeWatermark
	buffers    []*daemon.BufferInfo
	buffer     *daemon.BufferInfo
	metrics    []*daemon.VertexMetrics
	errors     []*daemon.ReplicaErrors
}

func (f *fakeDaemonClient) Close() error                    { return nil }
func (f *fakeDaemonClient) IsDrained(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (f *fakeDaemonClient) ListPipelineBuffers(_ context.Context, _ string) ([]*daemon.BufferInfo, error) {
	return f.buffers, nil
}
func (f *fakeDaemonClient) GetPipelineBuffer(_ context.Context, _, _ string) (*daemon.BufferInfo, error) {
	return f.buffer, nil
}
func (f *fakeDaemonClient) GetVertexMetrics(_ context.Context, _, _ string) ([]*daemon.VertexMetrics, error) {
	return f.metrics, nil
}
func (f *fakeDaemonClient) GetPipelineWatermarks(_ context.Context, _ string) ([]*daemon.EdgeWatermark, error) {
	return f.watermarks, nil
}
func (f *fakeDaemonClient) GetPipelineStatus(_ context.Context, _ string) (*daemon.PipelineStatus, error) {
	return f.status, nil
}
func (f *fakeDaemonClient) GetVertexErrors(_ context.Context, _, _ string) ([]*daemon.ReplicaErrors, error) {
	return f.errors, nil
}

var _ daemonclient.DaemonClient = (*fakeDaemonClient)(nil)

// fakeMVTDaemonClient implements MonoVertexDaemonClient for testing.
type fakeMVTDaemonClient struct {
	status  *mvtxdaemon.MonoVertexStatus
	metrics *mvtxdaemon.MonoVertexMetrics
	errors  []*mvtxdaemon.ReplicaErrors
}

func (f *fakeMVTDaemonClient) Close() error { return nil }
func (f *fakeMVTDaemonClient) GetMonoVertexMetrics(_ context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	return f.metrics, nil
}
func (f *fakeMVTDaemonClient) GetMonoVertexStatus(_ context.Context) (*mvtxdaemon.MonoVertexStatus, error) {
	return f.status, nil
}
func (f *fakeMVTDaemonClient) GetMonoVertexErrors(_ context.Context, _ string) ([]*mvtxdaemon.ReplicaErrors, error) {
	return f.errors, nil
}

var _ mvtdaemonclient.MonoVertexDaemonClient = (*fakeMVTDaemonClient)(nil)

// newDaemonTestRegistry injects fake daemon clients into the registry and
// returns the populated ToolRegistry together with the fake clients for
// assertion.
func newDaemonTestRegistry(t *testing.T) (ToolRegistry, *fakeDaemonClient, *fakeMVTDaemonClient) {
	t.Helper()

	fdc := &fakeDaemonClient{
		status:     &daemon.PipelineStatus{Status: "healthy", Message: "all good", Code: "200"},
		watermarks: []*daemon.EdgeWatermark{{Pipeline: "p1", Edge: "in-out", IsWatermarkEnabled: wrapperspb.Bool(true)}},
		buffers:    []*daemon.BufferInfo{{Pipeline: "p1", BufferName: "p1-in-out-0", PendingCount: wrapperspb.Int64(42)}},
		buffer:     &daemon.BufferInfo{Pipeline: "p1", BufferName: "p1-in-out-0", PendingCount: wrapperspb.Int64(42)},
		metrics:    []*daemon.VertexMetrics{{Pipeline: "p1", Vertex: "in"}},
		errors:     []*daemon.ReplicaErrors{},
	}
	fmdc := &fakeMVTDaemonClient{
		status:  &mvtxdaemon.MonoVertexStatus{Status: "healthy"},
		metrics: &mvtxdaemon.MonoVertexMetrics{MonoVertex: "mv1"},
		errors:  []*mvtxdaemon.ReplicaErrors{},
	}

	// Swap global factories for the duration of the test.
	origPl := daemonClientFactory
	origMv := mvtDaemonClientFactory
	t.Cleanup(func() {
		daemonClientFactory = origPl
		mvtDaemonClientFactory = origMv
	})
	daemonClientFactory = func(_, _ string) (daemonclient.DaemonClient, error) { return fdc, nil }
	mvtDaemonClientFactory = func(_, _ string) (mvtdaemonclient.MonoVertexDaemonClient, error) { return fmdc, nil }

	nf := dffake.NewSimpleClientset().NumaflowV1alpha1()
	kube := k8sfake.NewSimpleClientset()
	reg := NewRegistry(kube, nf, testNS, "grpc")
	return reg, fdc, fmdc
}

func TestGetPipelineStatus(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pipeline_status"), map[string]any{"namespace": testNS, "pipeline": "p1"})
	assert.False(t, res.IsError)
	var out daemon.PipelineStatus
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &out))
	assert.Equal(t, "healthy", out.Status)
}

func TestGetPipelineWatermarks(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pipeline_watermarks"), map[string]any{"namespace": testNS, "pipeline": "p1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "in-out")
}

func TestListBuffers(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_buffers"), map[string]any{"namespace": testNS, "pipeline": "p1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "p1-in-out-0")
}

func TestGetBufferInfo(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_buffer_info"), map[string]any{
		"namespace": testNS, "pipeline": "p1", "buffer": "p1-in-out-0",
	})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "p1-in-out-0")
}

func TestGetBufferInfo_MissingBuffer(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_buffer_info"), map[string]any{"namespace": testNS, "pipeline": "p1"})
	assert.True(t, res.IsError)
}

func TestGetVertexMetrics(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_vertex_metrics"), map[string]any{
		"namespace": testNS, "pipeline": "p1", "vertex": "in",
	})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), `"in"`)
}

func TestGetVertexErrors(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_vertex_errors"), map[string]any{
		"namespace": testNS, "pipeline": "p1", "vertex": "in",
	})
	assert.False(t, res.IsError)
	// empty error list marshals as []
	assert.Contains(t, textOf(t, res), "[]")
}

func TestGetMonoVertexStatus(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_monovertex_status"), map[string]any{"namespace": testNS, "mono_vertex": "mv1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "healthy")
}

func TestGetMonoVertexMetrics(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_monovertex_metrics"), map[string]any{"namespace": testNS, "mono_vertex": "mv1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "mv1")
}

func TestGetMonoVertexErrors(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_monovertex_errors"), map[string]any{"namespace": testNS, "mono_vertex": "mv1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "[]")
}

func TestDaemonTools_MissingPipeline(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	for _, tool := range []string{"get_pipeline_status", "get_pipeline_watermarks", "list_buffers", "get_vertex_metrics", "get_vertex_errors"} {
		res := call(t, handlerFor(t, reg, tool), map[string]any{"namespace": testNS})
		assert.True(t, res.IsError, "tool %q should error when pipeline arg is missing", tool)
	}
}

func TestDaemonTools_MissingMonoVertex(t *testing.T) {
	reg, _, _ := newDaemonTestRegistry(t)
	for _, tool := range []string{"get_monovertex_status", "get_monovertex_metrics", "get_monovertex_errors"} {
		res := call(t, handlerFor(t, reg, tool), map[string]any{"namespace": testNS})
		assert.True(t, res.IsError, "tool %q should error when mono_vertex arg is missing", tool)
	}
}

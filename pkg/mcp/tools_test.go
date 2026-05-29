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

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dffake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
)

const testNS = "test-ns"

func newTestRegistry(t *testing.T) ToolRegistry {
	t.Helper()
	pl := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: testNS},
		Spec:       dfv1.PipelineSpec{Vertices: []dfv1.AbstractVertex{{Name: "in"}, {Name: "out"}}},
		Status:     dfv1.PipelineStatus{Phase: dfv1.PipelinePhaseRunning},
	}
	mv := &dfv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "mv1", Namespace: testNS},
		Status:     dfv1.MonoVertexStatus{Phase: dfv1.MonoVertexPhaseRunning, Replicas: 2, ReadyReplicas: 2},
	}
	isb := &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNS},
		Spec:       dfv1.InterStepBufferServiceSpec{JetStream: &dfv1.JetStreamBufferService{}},
		Status:     dfv1.InterStepBufferServiceStatus{Phase: dfv1.ISBSvcPhaseRunning},
	}
	// Seed via the typed client's Create rather than NewSimpleClientset(objs...),
	// because the fake constructor mis-pluralizes irregular kinds (MonoVertex ->
	// "monovertexs") and the seeded object would not be found by List.
	client := dffake.NewSimpleClientset()
	nf := client.NumaflowV1alpha1()
	ctx := context.Background()
	_, err := nf.Pipelines(testNS).Create(ctx, pl, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = nf.MonoVertices(testNS).Create(ctx, mv, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = nf.InterStepBufferServices(testNS).Create(ctx, isb, metav1.CreateOptions{})
	require.NoError(t, err)
	kubeFake := k8sfake.NewSimpleClientset()
	return NewRegistry(kubeFake, nf, testNS, "grpc")
}

func handlerFor(t *testing.T, reg ToolRegistry, name string) server.ToolHandlerFunc {
	t.Helper()
	for _, td := range reg.Tools() {
		if td.Tool.Name == name {
			return td.Handler
		}
	}
	t.Fatalf("tool %q not registered", name)
	return nil
}

func call(t *testing.T, h server.ToolHandlerFunc, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, err := h(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, res)
	return res
}

func textOf(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	require.Len(t, res.Content, 1)
	tc, ok := res.Content[0].(mcp.TextContent)
	require.True(t, ok, "expected text content, got %T", res.Content[0])
	return tc.Text
}

func TestRegistry_IsReadOnly(t *testing.T) {
	reg := newTestRegistry(t)
	names := map[string]bool{}
	for _, td := range reg.Tools() {
		names[td.Tool.Name] = true
	}
	// 5 CRD tools + 9 daemon tools + 6 K8s tools = 20
	assert.Len(t, names, 20)
	for _, n := range []string{
		"list_pipelines", "get_pipeline", "list_monovertices", "get_monovertex", "list_isbservices",
		"get_pipeline_status", "get_pipeline_watermarks", "list_buffers", "get_buffer_info",
		"get_vertex_metrics", "get_vertex_errors",
		"get_monovertex_status", "get_monovertex_metrics", "get_monovertex_errors",
		"list_namespaces", "get_cluster_summary", "list_pods", "get_pod_info",
		"tail_pod_logs", "list_namespace_events",
	} {
		assert.Contains(t, names, n)
	}
	for n := range names {
		for _, verb := range []string{"create", "update", "delete", "patch", "pause", "resume"} {
			assert.NotContains(t, n, verb, "read-only registry must not expose mutating tools")
		}
	}
}

func TestListPipelines(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_pipelines"), map[string]any{"namespace": testNS})
	assert.False(t, res.IsError)
	var got []objectSummary
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "p1", got[0].Name)
	assert.Equal(t, "Running", got[0].Phase)
	require.NotNil(t, got[0].VertexCount)
	assert.Equal(t, uint32(2), *got[0].VertexCount)
}

func TestGetPipeline(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pipeline"), map[string]any{"namespace": testNS, "pipeline": "p1"})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), `"p1"`)
}

func TestGetPipeline_MissingName(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pipeline"), map[string]any{"namespace": testNS})
	assert.True(t, res.IsError)
}

func TestGetPipeline_NotFound(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pipeline"), map[string]any{"namespace": testNS, "pipeline": "missing"})
	assert.True(t, res.IsError)
}

func TestListMonoVertices(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_monovertices"), map[string]any{"namespace": testNS})
	assert.False(t, res.IsError)
	var got []objectSummary
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "mv1", got[0].Name)
	require.NotNil(t, got[0].Replicas)
	assert.Equal(t, uint32(2), *got[0].Replicas)
}

func TestListISBServices(t *testing.T) {
	reg := newTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_isbservices"), map[string]any{"namespace": testNS})
	assert.False(t, res.IsError)
	var got []objectSummary
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "default", got[0].Name)
	assert.Equal(t, "jetstream", got[0].Type)
}

func TestListPipelines_AllNamespaces(t *testing.T) {
	reg := newTestRegistry(t)
	// No namespace arg and default is testNS, so it still scopes to testNS here;
	// this asserts the default-namespace fallback path is exercised without error.
	res := call(t, handlerFor(t, reg, "list_pipelines"), map[string]any{})
	assert.False(t, res.IsError)
	var got []objectSummary
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &got))
	require.Len(t, got, 1)
}

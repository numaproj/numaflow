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

package v1

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeKube "k8s.io/client-go/kubernetes/fake"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

func TestMCPToolkitRegisteredTools(t *testing.T) {
	registry := NewMCPToolkit(&handler{opts: &handlerOptions{}})
	tools := registry.RegisteredTools()
	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Tool.Name] = true
	}

	expected := []string{
		"list_namespaces",
		"get_cluster_summary",
		"list_pipelines",
		"get_pipeline_topology",
		"get_pipeline_debug_snapshot",
		"get_pipeline_buffers",
		"get_pipeline_buffer",
		"get_pipeline_buffer_pending",
		"get_pipeline_edge_watermarks",
		"get_pipeline_watermark_lag",
		"get_vertex_metrics",
		"get_vertex_logs",
		"get_vertex_runtime_errors",
		"list_mono_vertices",
		"get_mono_vertex_debug_snapshot",
		"get_mono_vertex_throughput",
		"get_mono_vertex_logs",
		"get_mono_vertex_runtime_errors",
		"get_pod_logs",
		"discover_metrics",
	}
	for _, name := range expected {
		assert.Truef(t, names[name], "expected MCP tool %q to be registered", name)
	}

	removed := []string{
		"get_pipeline_isbs",
		"get_pipeline_watermarks",
		"get_vertices_metrics",
		"get_vertex_errors",
		"get_mono_vertex_metrics",
		"get_mono_vertex_errors",
		"query_metrics",
	}
	for _, name := range removed {
		assert.Falsef(t, names[name], "MCP tool %q should not be registered", name)
	}
}

func TestMCPToolkitDoesNotRegisterMutationTools(t *testing.T) {
	registry := NewMCPToolkit(&handler{opts: &handlerOptions{}})
	for _, tool := range registry.RegisteredTools() {
		name := strings.ToLower(tool.Tool.Name)
		for _, forbidden := range []string{"create", "update", "delete", "patch", "pause", "resume", "restart", "scale"} {
			assert.NotContains(t, name, forbidden)
		}
	}
}

func TestMCPToolkitLogToolsDoNotExposeFollow(t *testing.T) {
	registry := NewMCPToolkit(&handler{opts: &handlerOptions{}})
	foundLogTool := false
	for _, tool := range registry.RegisteredTools() {
		if !strings.Contains(tool.Tool.Name, "logs") {
			continue
		}
		foundLogTool = true
		assert.NotContains(t, tool.Tool.InputSchema.Properties, "follow")
	}
	require.True(t, foundLogTool)
}

func TestMCPToolkitToolsAreAnnotatedReadOnly(t *testing.T) {
	registry := NewMCPToolkit(&handler{opts: &handlerOptions{}})
	for _, tool := range registry.RegisteredTools() {
		assert.NotNil(t, tool.Tool.Annotations.ReadOnlyHint, tool.Tool.Name)
		assert.True(t, *tool.Tool.Annotations.ReadOnlyHint, tool.Tool.Name)
		assert.NotNil(t, tool.Tool.Annotations.DestructiveHint, tool.Tool.Name)
		assert.False(t, *tool.Tool.Annotations.DestructiveHint, tool.Tool.Name)
		assert.NotNil(t, tool.Tool.Annotations.IdempotentHint, tool.Tool.Name)
		assert.True(t, *tool.Tool.Annotations.IdempotentHint, tool.Tool.Name)
	}
}

func TestMCPToolkitRequiredSchemas(t *testing.T) {
	tools := mcpToolsByName(NewMCPToolkit(&handler{opts: &handlerOptions{}}).RegisteredTools())

	assert.ElementsMatch(t, []string{"namespace", "pipeline"}, tools["get_pipeline_debug_snapshot"].InputSchema.Required)
	assert.ElementsMatch(t, []string{"namespace", "pipeline", "vertex"}, tools["get_vertex_pending"].InputSchema.Required)
	assert.ElementsMatch(t, []string{"namespace", "pipeline", "buffer"}, tools["get_pipeline_buffer_pending"].InputSchema.Required)
	assert.ElementsMatch(t, []string{"namespace", "monoVertex"}, tools["get_mono_vertex_throughput"].InputSchema.Required)
}

func TestMCPToolkitRouteHandlerCapsTailAndLimit(t *testing.T) {
	var gotQuery map[string]string
	tk := &mcpToolkit{
		h: &handler{opts: &handlerOptions{}},
		invokeRouteFunc: func(_ context.Context, _, _ string, _ gin.Params, query map[string]string, _ string) (*mcp.CallToolResult, error) {
			gotQuery = query
			return mcp.NewToolResultText("{}"), nil
		},
	}
	_, err := tk.routeHandler("GET", "/logs", tailLinesArg)(context.Background(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Arguments: map[string]any{"tailLines": float64(5000)}},
	})
	require.NoError(t, err)
	assert.Equal(t, "1000", gotQuery["tailLines"])

	_, err = tk.routeHandler("GET", "/events", eventLimitArg)(context.Background(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Arguments: map[string]any{"limit": float64(-1)}},
	})
	require.NoError(t, err)
	assert.Equal(t, "1", gotQuery["limit"])
}

func TestMCPToolkitDispatchesRepresentativeAPIRoutes(t *testing.T) {
	t.Run("pipeline debug snapshot", func(t *testing.T) {
		h := pipelineDaemonHandler(t, &fakeDaemonClient{
			status:  &daemon.PipelineStatus{Status: "warning", Code: "D2"},
			buffers: []*daemon.BufferInfo{{BufferName: "p1-out-0", PendingCount: wrapperspb.Int64(7)}},
			metrics: []*daemon.VertexMetrics{{Vertex: "out", Pendings: map[string]*wrapperspb.Int64Value{
				"1m": wrapperspb.Int64(7),
			}}},
		}, twoVertexPipeline())
		result := callMCPTool(t, h, "get_pipeline_debug_snapshot", map[string]any{"namespace": "ns1", "pipeline": "p1"})
		var got DebugSnapshotDTO
		require.NoError(t, json.Unmarshal([]byte(mcpText(t, result)), &got))
		assert.NotNil(t, got.Buffers.Data)
	})

	t.Run("vertex pending", func(t *testing.T) {
		h := pipelineDaemonHandler(t, &fakeDaemonClient{metrics: []*daemon.VertexMetrics{{
			Vertex: "out",
			Pendings: map[string]*wrapperspb.Int64Value{
				"1m": wrapperspb.Int64(42),
			},
		}}}, twoVertexPipeline())
		result := callMCPTool(t, h, "get_vertex_pending", map[string]any{"namespace": "ns1", "pipeline": "p1", "vertex": "out"})
		var got PendingDTO
		require.NoError(t, json.Unmarshal([]byte(mcpText(t, result)), &got))
		require.Len(t, got.Partitions, 1)
		assert.Equal(t, int64(42), got.Partitions[0].Pending["1m"])
	})

	t.Run("mono vertex throughput", func(t *testing.T) {
		h := monoVertexDaemonHandler(t, &fakeMvtClient{metrics: &mvtxdaemon.MonoVertexMetrics{
			ProcessingRates: map[string]*wrapperspb.DoubleValue{"1m": wrapperspb.Double(2.5)},
			Pendings:        map[string]*wrapperspb.Int64Value{"1m": wrapperspb.Int64(9)},
		}}, monoVertex())
		result := callMCPTool(t, h, "get_mono_vertex_throughput", map[string]any{"namespace": "ns1", "monoVertex": "mv1"})
		var got MonoVertexMetricsDTO
		require.NoError(t, json.Unmarshal([]byte(mcpText(t, result)), &got))
		assert.Equal(t, 2.5, got.ProcessingRates["1m"])
		assert.Equal(t, int64(9), got.Pending["1m"])
	})

	t.Run("events", func(t *testing.T) {
		event := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "ns1", Name: "e1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pipeline", Name: "p1"},
			Type:           "Warning",
			Reason:         "BackOff",
			Message:        "still failing",
			LastTimestamp:  metav1.Now(),
		}
		h := &handler{kubeClient: fakeKube.NewSimpleClientset(event), opts: &handlerOptions{}}
		result := callMCPTool(t, h, "get_pipeline_events", map[string]any{"namespace": "ns1", "pipeline": "p1", "limit": float64(1)})
		var got []K8sEventsResponse
		require.NoError(t, json.Unmarshal([]byte(mcpText(t, result)), &got))
		require.Len(t, got, 1)
		assert.Equal(t, "BackOff", got[0].Reason)
	})
}

func TestBoundedInt(t *testing.T) {
	assert.Equal(t, 1, boundedInt(-10, 1, 100))
	assert.Equal(t, 50, boundedInt(50, 1, 100))
	assert.Equal(t, 100, boundedInt(500, 1, 100))
}

func mcpToolsByName(definitions []ToolDefinition) map[string]mcp.Tool {
	tools := make(map[string]mcp.Tool, len(definitions))
	for _, definition := range definitions {
		tools[definition.Tool.Name] = definition.Tool
	}
	return tools
}

func callMCPTool(t *testing.T, h *handler, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	for _, definition := range NewMCPToolkit(h).RegisteredTools() {
		if definition.Tool.Name == name {
			result, err := definition.Handler(context.Background(), mcp.CallToolRequest{Params: mcp.CallToolParams{Arguments: args}})
			require.NoError(t, err)
			require.False(t, result.IsError)
			return result
		}
	}
	t.Fatalf("MCP tool %q not found", name)
	return nil
}

func mcpText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.Len(t, result.Content, 1)
	text, ok := mcp.AsTextContent(result.Content[0])
	require.True(t, ok)
	return text.Text
}

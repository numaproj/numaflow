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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/numaproj/numaflow"
)

const (
	mcpDefaultLogTailLines = 200
	mcpMaxLogTailLines     = 1000
	mcpDefaultEventLimit   = 100
	mcpMaxEventLimit       = 500
)

type ToolRegistry interface {
	RegisteredTools() []ToolDefinition
}

type ToolDefinition struct {
	Tool    mcp.Tool
	Handler mcpserver.ToolHandlerFunc
}

type mcpToolkit struct {
	h               *handler
	invokeRouteFunc func(context.Context, string, string, gin.Params, map[string]string, string) (*mcp.CallToolResult, error)
}

func NewMCPServerFromRegistry(registry ToolRegistry) *mcpserver.MCPServer {
	srv := mcpserver.NewMCPServer("numaflow-debug-mcp", "0.1.0")
	for _, tool := range registry.RegisteredTools() {
		srv.AddTool(tool.Tool, tool.Handler)
	}
	return srv
}

func NewMCPToolkit(h *handler) ToolRegistry {
	return &mcpToolkit{h: h}
}

var _ ToolRegistry = (*mcpToolkit)(nil)

func (tk *mcpToolkit) RegisteredTools() []ToolDefinition {
	return []ToolDefinition{
		tk.sysInfoTool(),
		tk.routeTool("get_authinfo", "Get Numaflow auth information", http.MethodGet, "/authinfo"),
		tk.routeTool("list_namespaces", "List namespaces containing Numaflow resources", http.MethodGet, "/namespaces"),
		tk.routeTool("get_cluster_summary", "Get cluster summary across namespaces", http.MethodGet, "/cluster-summary"),
		tk.routeTool("get_namespace_events", "Get bounded Kubernetes events for a namespace", http.MethodGet, "/namespaces/:namespace/events", namespaceArg, objectTypeArg, objectNameArg, eventLimitArg),
		tk.routeTool("list_pipelines", "List pipelines in a namespace", http.MethodGet, "/namespaces/:namespace/pipelines", namespaceArg),
		tk.routeTool("get_pipeline", "Get a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline", namespaceArg, pipelineArg),
		tk.routeTool("get_pipeline_health", "Get pipeline health", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/health", namespaceArg, pipelineArg),
		tk.routeTool("list_vertex_pods", "List pods for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_vertex_pods_info", "Get detailed pod information for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods-info", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_pod_metrics", "Get pod CPU and memory metrics for a namespace", http.MethodGet, "/metrics/namespaces/:namespace/pods", namespaceArg),
		tk.routeTool("get_pod_logs", "Get bounded recent logs for a pod", http.MethodGet, "/namespaces/:namespace/pods/:pod/logs", namespaceArg, podArg, containerArg, previousArg, tailLinesArg),
		tk.routeTool("list_isb_services", "List InterStepBufferService objects in a namespace", http.MethodGet, "/namespaces/:namespace/isb-services", namespaceArg),
		tk.routeTool("get_isb_service", "Get an InterStepBufferService object", http.MethodGet, "/namespaces/:namespace/isb-services/:isb-service", namespaceArg, isbServiceArg),
		tk.routeTool("list_mono_vertices", "List mono-vertices in a namespace", http.MethodGet, "/namespaces/:namespace/mono-vertices", namespaceArg),
		tk.routeTool("get_mono_vertex", "Get a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex", namespaceArg, monoVertexArg),
		tk.routeTool("list_mono_vertex_pods", "List pods for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/pods", namespaceArg, monoVertexArg),
		tk.routeTool("get_mono_vertex_health", "Get mono-vertex health", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/health", namespaceArg, monoVertexArg),
		tk.routeTool("discover_metrics", "Discover configured metrics for an object type", http.MethodGet, "/metrics-discovery/object/:object", objectArg),
		tk.routeTool("get_vertex_metrics", "Get metrics for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/metrics", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_vertex_pending", "Get pending counts for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pending", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_mono_vertex_pending", "Get pending counts for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/pending", namespaceArg, monoVertexArg),
		tk.routeTool("get_pipeline_buffer", "Get metrics for a pipeline buffer", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/buffers/:buffer", namespaceArg, pipelineArg, bufferArg),
		tk.routeTool("get_pipeline_buffer_pending", "Get pending counts for a pipeline buffer", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/buffers/:buffer/pending", namespaceArg, pipelineArg, bufferArg),
		tk.routeTool("get_pipeline_data_health", "Get daemon-reported data health for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/data-health", namespaceArg, pipelineArg),
		tk.routeTool("get_mono_vertex_data_health", "Get daemon-reported data health for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/data-health", namespaceArg, monoVertexArg),
		tk.routeTool("get_pipeline_watermark_lag", "Get end-to-end watermark lag for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/watermark-lag", namespaceArg, pipelineArg),
		tk.routeTool("get_pipeline_status", "Get status for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/status", namespaceArg, pipelineArg),
		tk.routeTool("get_vertex_status", "Get status for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/status", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_mono_vertex_status", "Get status for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/status", namespaceArg, monoVertexArg),
		tk.routeTool("get_vertex_logs", "Get bounded recent logs for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/logs", namespaceArg, pipelineArg, vertexArg, containerArg, replicaArg, previousArg, tailLinesArg),
		tk.routeTool("get_mono_vertex_logs", "Get bounded recent logs for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/logs", namespaceArg, monoVertexArg, containerArg, replicaArg, previousArg, tailLinesArg),
		tk.routeTool("get_vertex_runtime_errors", "Get runtime errors for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/runtime-errors", namespaceArg, pipelineArg, vertexArg),
		tk.routeTool("get_mono_vertex_runtime_errors", "Get runtime errors for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/runtime-errors", namespaceArg, monoVertexArg),
		tk.routeTool("get_pipeline_topology", "Get graph topology for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/topology", namespaceArg, pipelineArg),
		tk.routeTool("get_pipeline_debug_snapshot", "Get a bounded pipeline debug snapshot", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/debug-snapshot", namespaceArg, pipelineArg),
		tk.routeTool("get_mono_vertex_debug_snapshot", "Get a bounded mono-vertex debug snapshot", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/debug-snapshot", namespaceArg, monoVertexArg),
		tk.routeTool("get_pipeline_buffers", "Get typed pipeline buffer information", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/buffers", namespaceArg, pipelineArg),
		tk.routeTool("get_pipeline_edge_watermarks", "Get typed per-edge watermarks for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/edge-watermarks", namespaceArg, pipelineArg),
		tk.routeTool("get_mono_vertex_throughput", "Get throughput and pending metrics for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/throughput", namespaceArg, monoVertexArg),
		tk.routeTool("get_pipeline_events", "Get bounded events for a pipeline", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/events", namespaceArg, pipelineArg, eventLimitArg),
		tk.routeTool("get_vertex_events", "Get bounded events for a pipeline vertex", http.MethodGet, "/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/events", namespaceArg, pipelineArg, vertexArg, eventLimitArg),
		tk.routeTool("get_mono_vertex_events", "Get bounded events for a mono-vertex", http.MethodGet, "/namespaces/:namespace/mono-vertices/:mono-vertex/events", namespaceArg, monoVertexArg, eventLimitArg),
	}
}

func (tk *mcpToolkit) sysInfoTool() ToolDefinition {
	return ToolDefinition{
		Tool: mcp.NewTool("get_sysinfo", readOnlyToolOptions("Get basic Numaflow MCP server information")...),
		Handler: func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			daemonClientProtocol := "grpc"
			if tk.h != nil && tk.h.opts != nil && tk.h.opts.daemonClientProtocol != "" {
				daemonClientProtocol = tk.h.opts.daemonClientProtocol
			}
			data := map[string]any{
				"name":                 "numaflow-debug-mcp",
				"version":              numaflow.GetVersion().String(),
				"readOnly":             true,
				"daemonClientProtocol": daemonClientProtocol,
				"logTail":              mcpDefaultLogTailLines,
				"maxLogTail":           mcpMaxLogTailLines,
				"eventLimit":           mcpDefaultEventLimit,
				"maxEventCap":          mcpMaxEventLimit,
			}
			bs, err := json.Marshal(data)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(string(bs)), nil
		},
	}
}

type toolArg struct {
	name        string
	description string
	required    bool
	kind        string
}

var (
	namespaceArg    = toolArg{name: "namespace", description: "Kubernetes namespace", required: true, kind: "string"}
	pipelineArg     = toolArg{name: "pipeline", description: "Pipeline name", required: true, kind: "string"}
	vertexArg       = toolArg{name: "vertex", description: "Pipeline vertex name", required: true, kind: "string"}
	monoVertexArg   = toolArg{name: "monoVertex", description: "MonoVertex name", required: true, kind: "string"}
	bufferArg       = toolArg{name: "buffer", description: "Buffer name", required: true, kind: "string"}
	podArg          = toolArg{name: "pod", description: "Pod name", required: true, kind: "string"}
	objectArg       = toolArg{name: "object", description: "Metric discovery object type", required: true, kind: "string"}
	isbServiceArg   = toolArg{name: "isbService", description: "InterStepBufferService name", required: true, kind: "string"}
	objectTypeArg   = toolArg{name: "objectType", description: "Optional Kubernetes involved object kind", kind: "string"}
	objectNameArg   = toolArg{name: "objectName", description: "Optional Kubernetes involved object name", kind: "string"}
	containerArg    = toolArg{name: "container", description: "Optional container name", kind: "string"}
	replicaArg      = toolArg{name: "replica", description: "Optional replica pod name", kind: "string"}
	previousArg     = toolArg{name: "previous", description: "Read previous terminated container logs", kind: "boolean"}
	tailLinesArg    = toolArg{name: "tailLines", description: "Number of recent log lines, capped by the server", kind: "number"}
	eventLimitArg   = toolArg{name: "limit", description: "Maximum events to return, capped by the server", kind: "number"}
)

func (tk *mcpToolkit) routeTool(name, description, method, path string, args ...toolArg) ToolDefinition {
	opts := readOnlyToolOptions(description)
	for _, arg := range args {
		propertyOpts := []mcp.PropertyOption{mcp.Description(arg.description)}
		if arg.required {
			propertyOpts = append(propertyOpts, mcp.Required())
		}
		switch arg.kind {
		case "number":
			opts = append(opts, mcp.WithNumber(arg.name, propertyOpts...))
		case "boolean":
			opts = append(opts, mcp.WithBoolean(arg.name, propertyOpts...))
		default:
			opts = append(opts, mcp.WithString(arg.name, propertyOpts...))
		}
	}
	return ToolDefinition{
		Tool:    mcp.NewTool(name, opts...),
		Handler: tk.routeHandler(method, path, args...),
	}
}

func (tk *mcpToolkit) routeHandler(method, path string, args ...toolArg) mcpserver.ToolHandlerFunc {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		params := gin.Params{}
		query := make(map[string]string)
		body := ""
		resolvedPath := path
		for _, arg := range args {
			if arg.required {
				if _, err := request.RequireString(arg.name); err != nil && arg.kind == "string" {
					return mcp.NewToolResultError(fmt.Sprintf("%s is required", arg.name)), nil
				}
			}
			switch arg.name {
			case "namespace", "pipeline", "vertex", "buffer", "pod", "object":
				value := request.GetString(arg.name, "")
				if value == "" && arg.required {
					return mcp.NewToolResultError(fmt.Sprintf("%s is required", arg.name)), nil
				}
				resolvedPath = strings.ReplaceAll(resolvedPath, ":"+arg.name, value)
				params = append(params, gin.Param{Key: arg.name, Value: value})
			case "monoVertex":
				value := request.GetString(arg.name, "")
				if value == "" && arg.required {
					return mcp.NewToolResultError("monoVertex is required"), nil
				}
				resolvedPath = strings.ReplaceAll(resolvedPath, ":mono-vertex", value)
				params = append(params, gin.Param{Key: "mono-vertex", Value: value})
			case "isbService":
				value := request.GetString(arg.name, "")
				if value == "" && arg.required {
					return mcp.NewToolResultError("isbService is required"), nil
				}
				resolvedPath = strings.ReplaceAll(resolvedPath, ":isb-service", value)
				params = append(params, gin.Param{Key: "isb-service", Value: value})
			case "tailLines":
				query["tailLines"] = strconv.Itoa(boundedInt(request.GetInt("tailLines", mcpDefaultLogTailLines), 1, mcpMaxLogTailLines))
			case "limit":
				query["limit"] = strconv.Itoa(boundedInt(request.GetInt("limit", mcpDefaultEventLimit), 1, mcpMaxEventLimit))
			case "previous":
				if request.GetBool("previous", false) {
					query["previous"] = "true"
				}
			case "query":
				body = request.GetString("query", "")
				if body == "" && arg.required {
					return mcp.NewToolResultError("query is required"), nil
				}
			default:
				if value := request.GetString(arg.name, ""); value != "" {
					query[arg.name] = value
				}
			}
		}
		if tk.invokeRouteFunc != nil {
			return tk.invokeRouteFunc(ctx, method, resolvedPath, params, query, body)
		}
		return tk.invokeRoute(ctx, method, resolvedPath, params, query, body)
	}
}

func (tk *mcpToolkit) invokeRoute(ctx context.Context, method, path string, params gin.Params, query map[string]string, body string) (*mcp.CallToolResult, error) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	requestBody := strings.NewReader(body)
	req, err := http.NewRequestWithContext(ctx, method, path, requestBody)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	q := req.URL.Query()
	for key, value := range query {
		q.Set(key, value)
	}
	req.URL.RawQuery = q.Encode()
	c.Request = req
	c.Params = params
	tk.dispatch(c, method, path)
	result := strings.TrimSpace(w.Body.String())
	if result == "" {
		result = http.StatusText(w.Code)
	}
	if w.Code >= http.StatusBadRequest {
		return mcp.NewToolResultError(result), nil
	}
	return mcp.NewToolResultText(result), nil
}

func (tk *mcpToolkit) dispatch(c *gin.Context, method, path string) {
	switch {
	case method == http.MethodGet && path == "/authinfo":
		tk.h.AuthInfo(c)
	case method == http.MethodGet && path == "/namespaces":
		tk.h.ListNamespaces(c)
	case method == http.MethodGet && path == "/cluster-summary":
		tk.h.GetClusterSummary(c)
	case method == http.MethodGet && strings.HasSuffix(path, "/events"):
		tk.dispatchEvent(c, path)
	case method == http.MethodGet && strings.HasSuffix(path, "/pipelines"):
		tk.h.ListPipelines(c)
	case method == http.MethodGet && strings.Contains(path, "/pipelines/"):
		tk.dispatchPipeline(c, path)
	case method == http.MethodGet && strings.HasSuffix(path, "/mono-vertices"):
		tk.h.ListMonoVertices(c)
	case method == http.MethodGet && strings.Contains(path, "/mono-vertices/"):
		tk.dispatchMonoVertex(c, path)
	case method == http.MethodGet && strings.Contains(path, "/isb-services/"):
		tk.h.GetInterStepBufferService(c)
	case method == http.MethodGet && strings.HasSuffix(path, "/isb-services"):
		tk.h.ListInterStepBufferServices(c)
	case method == http.MethodGet && strings.Contains(path, "/pods/") && strings.HasSuffix(path, "/logs"):
		tk.h.PodLogs(c)
	case method == http.MethodGet && strings.HasPrefix(path, "/metrics/namespaces/"):
		tk.h.ListPodsMetrics(c)
	case method == http.MethodGet && strings.HasPrefix(path, "/metrics-discovery/object/"):
		tk.h.DiscoverMetrics(c)
	case method == http.MethodPost && path == "/metrics-proxy":
		tk.h.GetMetricData(c)
	default:
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("unsupported MCP route %s %s", method, path)})
	}
}

func (tk *mcpToolkit) dispatchPipeline(c *gin.Context, path string) {
	switch {
	case strings.HasSuffix(path, "/debug-snapshot"):
		tk.h.GetPipelineDebugSnapshot(c)
	case strings.HasSuffix(path, "/topology"):
		tk.h.GetPipelineTopology(c)
	case strings.HasSuffix(path, "/edge-watermarks"):
		tk.h.GetPipelineEdgeWatermarks(c)
	case strings.HasSuffix(path, "/watermark-lag"):
		tk.h.GetPipelineWatermarkLag(c)
	case strings.HasSuffix(path, "/watermarks"):
		tk.h.GetPipelineWatermarks(c)
	case strings.HasSuffix(path, "/data-health"):
		tk.h.GetPipelineDataHealth(c)
	case strings.HasSuffix(path, "/status"):
		tk.h.GetPipelineStatusInfo(c)
	case strings.HasSuffix(path, "/health"):
		tk.h.GetPipelineStatus(c)
	case strings.HasSuffix(path, "/isbs"):
		tk.h.ListPipelineBuffers(c)
	case strings.HasSuffix(path, "/vertices/metrics"):
		tk.h.GetVerticesMetrics(c)
	case strings.Contains(path, "/vertices/"):
		tk.dispatchVertex(c, path)
	case strings.Contains(path, "/buffers/") && strings.HasSuffix(path, "/pending"):
		tk.h.GetPipelineBufferPending(c)
	case strings.Contains(path, "/buffers/"):
		tk.h.GetPipelineBufferInfo(c)
	case strings.HasSuffix(path, "/buffers"):
		tk.h.GetPipelineBuffers(c)
	default:
		tk.h.GetPipeline(c)
	}
}

func (tk *mcpToolkit) dispatchVertex(c *gin.Context, path string) {
	switch {
	case strings.HasSuffix(path, "/pods-info"):
		tk.h.GetVertexPodsInfo(c)
	case strings.HasSuffix(path, "/pods"):
		tk.h.ListVertexPods(c)
	case strings.HasSuffix(path, "/metrics"):
		tk.h.GetVertexMetrics(c)
	case strings.HasSuffix(path, "/pending"):
		tk.h.GetVertexPending(c)
	case strings.HasSuffix(path, "/status"):
		tk.h.GetVertexStatus(c)
	case strings.HasSuffix(path, "/logs"):
		tk.h.GetVertexLogs(c)
	case strings.HasSuffix(path, "/runtime-errors"):
		tk.h.GetVertexRuntimeErrors(c)
	case strings.HasSuffix(path, "/errors"):
		tk.h.GetVertexErrors(c)
	case strings.HasSuffix(path, "/events"):
		tk.h.GetVertexEvents(c)
	default:
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("unsupported vertex MCP route %s", path)})
	}
}

func (tk *mcpToolkit) dispatchMonoVertex(c *gin.Context, path string) {
	switch {
	case strings.HasSuffix(path, "/debug-snapshot"):
		tk.h.GetMonoVertexDebugSnapshot(c)
	case strings.HasSuffix(path, "/throughput"):
		tk.h.GetMonoVertexThroughputMetrics(c)
	case strings.HasSuffix(path, "/data-health"):
		tk.h.GetMonoVertexDataHealth(c)
	case strings.HasSuffix(path, "/status"):
		tk.h.GetMonoVertexStatusInfo(c)
	case strings.HasSuffix(path, "/health"):
		tk.h.GetMonoVertexHealth(c)
	case strings.HasSuffix(path, "/metrics"):
		tk.h.GetMonoVertexMetrics(c)
	case strings.HasSuffix(path, "/pending"):
		tk.h.GetMonoVertexPending(c)
	case strings.HasSuffix(path, "/pods-info"):
		tk.h.GetMonoVertexPodsInfo(c)
	case strings.HasSuffix(path, "/pods"):
		tk.h.ListMonoVertexPods(c)
	case strings.HasSuffix(path, "/logs"):
		tk.h.GetMonoVertexLogs(c)
	case strings.HasSuffix(path, "/runtime-errors"):
		tk.h.GetMonoVertexRuntimeErrors(c)
	case strings.HasSuffix(path, "/errors"):
		tk.h.GetMonoVertexErrors(c)
	case strings.HasSuffix(path, "/events"):
		tk.h.GetMonoVertexEvents(c)
	default:
		tk.h.GetMonoVertex(c)
	}
}

func (tk *mcpToolkit) dispatchEvent(c *gin.Context, path string) {
	switch {
	case strings.Contains(path, "/vertices/"):
		tk.h.GetVertexEvents(c)
	case strings.Contains(path, "/pipelines/"):
		tk.h.GetPipelineEvents(c)
	case strings.Contains(path, "/mono-vertices/"):
		tk.h.GetMonoVertexEvents(c)
	default:
		tk.h.GetNamespaceEvents(c)
	}
}

func readOnlyToolOptions(description string) []mcp.ToolOption {
	return []mcp.ToolOption{
		mcp.WithDescription(description),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	}
}

func boundedInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

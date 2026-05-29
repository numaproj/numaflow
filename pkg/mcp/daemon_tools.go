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
	"fmt"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mark3labs/mcp-go/mcp"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

// daemonClientFactory and mvtDaemonClientFactory are package-level vars so tests
// can swap them for fakes without touching the registry constructor.
var (
	daemonClientFactory = func(addr, protocol string) (daemonclient.DaemonClient, error) {
		if strings.EqualFold(protocol, "http") {
			return daemonclient.NewRESTfulDaemonServiceClient(addr)
		}
		return daemonclient.NewGRPCDaemonServiceClient(addr)
	}
	mvtDaemonClientFactory = func(addr, protocol string) (mvtdaemonclient.MonoVertexDaemonClient, error) {
		if strings.EqualFold(protocol, "http") {
			return mvtdaemonclient.NewRESTfulClient(addr)
		}
		return mvtdaemonclient.NewGRPCClient(addr)
	}
)

// pipelineDaemonSvcAddress returns the in-cluster DNS address of the daemon
// service for a Pipeline.  The format must stay in sync with
// GetDaemonServiceURL in pkg/apis/numaflow/v1alpha1/pipeline_types.go.
func pipelineDaemonSvcAddress(ns, pipeline string) string {
	return fmt.Sprintf("%s-daemon-svc.%s.svc:%d", pipeline, ns, dfv1.DaemonServicePort)
}

// monoVertexDaemonSvcAddress returns the in-cluster DNS address of the daemon
// service for a MonoVertex.  Must stay in sync with GetDaemonServiceURL in
// pkg/apis/numaflow/v1alpha1/mono_vertex_types.go.
func monoVertexDaemonSvcAddress(ns, monoVertex string) string {
	return fmt.Sprintf("%s-mv-daemon-svc.%s.svc:%d", monoVertex, ns, dfv1.MonoVertexDaemonServicePort)
}

// daemonRegistry extends registry with LRU-cached daemon clients and the
// Kubernetes client needed for K8s-backed tools. It is created by
// NewRegistry when the full set of clients is provided.
type daemonRegistry struct {
	registry
	daemonClientsCache    *lru.Cache[string, daemonclient.DaemonClient]
	mvtDaemonClientsCache *lru.Cache[string, mvtdaemonclient.MonoVertexDaemonClient]
	daemonProtocol        string
}

// getPipelineDaemonClient returns a cached or freshly-dialled daemon client
// for the named pipeline.
func (r *daemonRegistry) getPipelineDaemonClient(ns, pipeline string) (daemonclient.DaemonClient, error) {
	addr := pipelineDaemonSvcAddress(ns, pipeline)
	if c, ok := r.daemonClientsCache.Get(addr); ok {
		return c, nil
	}
	c, err := daemonClientFactory(addr, r.daemonProtocol)
	if err != nil {
		return nil, err
	}
	r.daemonClientsCache.Add(addr, c)
	return c, nil
}

// getMonoVertexDaemonClient returns a cached or freshly-dialled daemon client
// for the named MonoVertex.
func (r *daemonRegistry) getMonoVertexDaemonClient(ns, monoVertex string) (mvtdaemonclient.MonoVertexDaemonClient, error) {
	addr := monoVertexDaemonSvcAddress(ns, monoVertex)
	if c, ok := r.mvtDaemonClientsCache.Get(addr); ok {
		return c, nil
	}
	c, err := mvtDaemonClientFactory(addr, r.daemonProtocol)
	if err != nil {
		return nil, err
	}
	r.mvtDaemonClientsCache.Add(addr, c)
	return c, nil
}

// daemonToolDefinitions returns the tool definitions for all daemon-backed
// read-only runtime diagnostic tools.
func (r *daemonRegistry) daemonToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{
			Tool: readOnlyTool("get_pipeline_status",
				mcp.WithDescription("Get the overall health and data-criticality status of a Numaflow Pipeline as reported by its daemon. Returns {status, message, code}."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline. If omitted, the server's default namespace is used.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
			),
			Handler: r.getPipelineStatus,
		},
		{
			Tool: readOnlyTool("get_pipeline_watermarks",
				mcp.WithDescription("Get per-edge watermarks for a Numaflow Pipeline. Returns an array of {pipeline, edge, watermarks, isWatermarkEnabled, from, to}."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
			),
			Handler: r.getPipelineWatermarks,
		},
		{
			Tool: readOnlyTool("list_buffers",
				mcp.WithDescription("List all inter-step buffers for a Numaflow Pipeline. Each entry includes pending, ack-pending, total messages, usage ratio and whether the buffer is full."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
			),
			Handler: r.listBuffers,
		},
		{
			Tool: readOnlyTool("get_buffer_info",
				mcp.WithDescription("Get detailed buffer metrics (pending, ack-pending, total, usage, isFull) for a single inter-step buffer of a Numaflow Pipeline."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
				mcp.WithString("buffer", mcp.Required(), mcp.Description("Name of the inter-step buffer (e.g. 'simple-pipeline-cat-0').")),
			),
			Handler: r.getBufferInfo,
		},
		{
			Tool: readOnlyTool("get_vertex_metrics",
				mcp.WithDescription("Get throughput and pending-count metrics for a vertex (or all vertices) of a Numaflow Pipeline. Returns processing rates keyed by rate window (e.g. '1m', '5m', '15m') and pending counts per replica."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
				mcp.WithString("vertex", mcp.Required(), mcp.Description("Name of the vertex.")),
			),
			Handler: r.getVertexMetrics,
		},
		{
			Tool: readOnlyTool("get_vertex_errors",
				mcp.WithDescription("Get structured runtime errors persisted by the monitor sidecar for a vertex of a Numaflow Pipeline. Returns an array of {replica, containerErrors:[{containerName, error, count, lastSeen}]}."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
				mcp.WithString("vertex", mcp.Required(), mcp.Description("Name of the vertex.")),
			),
			Handler: r.getVertexErrors,
		},
		{
			Tool: readOnlyTool("get_monovertex_status",
				mcp.WithDescription("Get the health status of a Numaflow MonoVertex as reported by its daemon. Returns {status, message, code}."),
				mcp.WithString("namespace", mcp.Description("Namespace of the MonoVertex.")),
				mcp.WithString("mono_vertex", mcp.Required(), mcp.Description("Name of the MonoVertex.")),
			),
			Handler: r.getMonoVertexStatus,
		},
		{
			Tool: readOnlyTool("get_monovertex_metrics",
				mcp.WithDescription("Get throughput and pending-count metrics for a Numaflow MonoVertex. Returns processing rates keyed by rate window and pending counts per replica."),
				mcp.WithString("namespace", mcp.Description("Namespace of the MonoVertex.")),
				mcp.WithString("mono_vertex", mcp.Required(), mcp.Description("Name of the MonoVertex.")),
			),
			Handler: r.getMonoVertexMetrics,
		},
		{
			Tool: readOnlyTool("get_monovertex_errors",
				mcp.WithDescription("Get structured runtime errors persisted by the monitor sidecar for a Numaflow MonoVertex. Returns an array of {replica, containerErrors}."),
				mcp.WithString("namespace", mcp.Description("Namespace of the MonoVertex.")),
				mcp.WithString("mono_vertex", mcp.Required(), mcp.Description("Name of the MonoVertex.")),
			),
			Handler: r.getMonoVertexErrors,
		},
	}
}

func (r *daemonRegistry) getPipelineStatus(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	status, err := c.GetPipelineStatus(ctx, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetPipelineStatus failed for %q: %v", pl, err)), nil
	}
	return jsonResult(status)
}

func (r *daemonRegistry) getPipelineWatermarks(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	wms, err := c.GetPipelineWatermarks(ctx, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetPipelineWatermarks failed for %q: %v", pl, err)), nil
	}
	return jsonResult(wms)
}

func (r *daemonRegistry) listBuffers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	buffers, err := c.ListPipelineBuffers(ctx, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("ListPipelineBuffers failed for %q: %v", pl, err)), nil
	}
	return jsonResult(buffers)
}

func (r *daemonRegistry) getBufferInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	bufferName, err := req.RequireString("buffer")
	if err != nil {
		return mcp.NewToolResultError("buffer is required"), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	info, err := c.GetPipelineBuffer(ctx, pl, bufferName)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetPipelineBuffer failed for %q/%q: %v", pl, bufferName, err)), nil
	}
	return jsonResult(info)
}

func (r *daemonRegistry) getVertexMetrics(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	vertex, err := req.RequireString("vertex")
	if err != nil {
		return mcp.NewToolResultError("vertex is required"), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	metrics, err := c.GetVertexMetrics(ctx, pl, vertex)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetVertexMetrics failed for %q/%q: %v", pl, vertex, err)), nil
	}
	return jsonResult(metrics)
}

func (r *daemonRegistry) getVertexErrors(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, pl, err := r.requirePipeline(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	vertex, err := req.RequireString("vertex")
	if err != nil {
		return mcp.NewToolResultError("vertex is required"), nil
	}
	c, err := r.getPipelineDaemonClient(ns, pl)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for pipeline %q: %v", pl, err)), nil
	}
	errs, err := c.GetVertexErrors(ctx, pl, vertex)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetVertexErrors failed for %q/%q: %v", pl, vertex, err)), nil
	}
	return jsonResult(errs)
}

func (r *daemonRegistry) getMonoVertexStatus(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, mv, err := r.requireMonoVertex(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getMonoVertexDaemonClient(ns, mv)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for mono vertex %q: %v", mv, err)), nil
	}
	status, err := c.GetMonoVertexStatus(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetMonoVertexStatus failed for %q: %v", mv, err)), nil
	}
	return jsonResult(status)
}

func (r *daemonRegistry) getMonoVertexMetrics(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, mv, err := r.requireMonoVertex(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getMonoVertexDaemonClient(ns, mv)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for mono vertex %q: %v", mv, err)), nil
	}
	metrics, err := c.GetMonoVertexMetrics(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetMonoVertexMetrics failed for %q: %v", mv, err)), nil
	}
	return jsonResult(metrics)
}

func (r *daemonRegistry) getMonoVertexErrors(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, mv, err := r.requireMonoVertex(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	c, err := r.getMonoVertexDaemonClient(ns, mv)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get daemon client for mono vertex %q: %v", mv, err)), nil
	}
	errs, err := c.GetMonoVertexErrors(ctx, mv)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("GetMonoVertexErrors failed for %q: %v", mv, err)), nil
	}
	return jsonResult(errs)
}

// requirePipeline extracts and validates the namespace + pipeline arguments.
func (r *daemonRegistry) requirePipeline(req mcp.CallToolRequest) (ns, pipeline string, err error) {
	pipeline, err = req.RequireString("pipeline")
	if err != nil {
		return "", "", fmt.Errorf("pipeline is required")
	}
	ns = r.namespace(req)
	if ns == "" {
		return "", "", fmt.Errorf("namespace is required (no default namespace configured)")
	}
	return ns, pipeline, nil
}

// requireMonoVertex extracts and validates the namespace + mono_vertex arguments.
func (r *daemonRegistry) requireMonoVertex(req mcp.CallToolRequest) (ns, monoVertex string, err error) {
	monoVertex, err = req.RequireString("mono_vertex")
	if err != nil {
		return "", "", fmt.Errorf("mono_vertex is required")
	}
	ns = r.namespace(req)
	if ns == "" {
		return "", "", fmt.Errorf("namespace is required (no default namespace configured)")
	}
	return ns, monoVertex, nil
}

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
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

// ToolDefinition pairs an MCP tool with the handler that serves it.
type ToolDefinition struct {
	Tool    mcp.Tool
	Handler server.ToolHandlerFunc
}

// ToolRegistry exposes the set of MCP tools that the server registers. Keeping
// this transport-agnostic means the same tools can later be served over a
// different transport (e.g. HTTP/SSE) without changes.
type ToolRegistry interface {
	Tools() []ToolDefinition
}

// registry is the read-only Numaflow tool registry, backed by the Numaflow
// typed clientset. It is read-only by construction: only list and get
// operations are registered, never create/update/delete/patch.
type registry struct {
	numaflowClient dfv1clients.NumaflowV1alpha1Interface
	// defaultNamespace is used when a tool call omits the "namespace" argument.
	// An empty value means "all namespaces" for list operations.
	defaultNamespace string
}

// NewRegistry creates a read-only Numaflow tool registry.
func NewRegistry(numaflowClient dfv1clients.NumaflowV1alpha1Interface, defaultNamespace string) ToolRegistry {
	return &registry{
		numaflowClient:   numaflowClient,
		defaultNamespace: defaultNamespace,
	}
}

var _ ToolRegistry = (*registry)(nil)

// readOnlyTool builds an MCP tool that is annotated as read-only and
// non-destructive, so clients render and gate it appropriately. Every tool in
// this registry is read-only, so all of them are constructed via this helper.
func readOnlyTool(name string, opts ...mcp.ToolOption) mcp.Tool {
	base := []mcp.ToolOption{
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	}
	return mcp.NewTool(name, append(base, opts...)...)
}

// Tools returns the read-only tool set, sorted implicitly by registration order.
func (r *registry) Tools() []ToolDefinition {
	return []ToolDefinition{
		{
			Tool: readOnlyTool("list_pipelines",
				mcp.WithDescription("List all Numaflow Pipelines in a namespace. Each entry includes the pipeline's phase (e.g. Running, Paused, Failed), an optional status message and its vertex count."),
				mcp.WithString("namespace", mcp.Description("Kubernetes namespace to list Pipelines in. If omitted, the server's default namespace is used (empty default means all namespaces).")),
			),
			Handler: r.listPipelines,
		},
		{
			Tool: readOnlyTool("get_pipeline",
				mcp.WithDescription("Get the full spec and status of a single Numaflow Pipeline."),
				mcp.WithString("namespace", mcp.Description("Namespace of the Pipeline. If omitted, the server's default namespace is used.")),
				mcp.WithString("pipeline", mcp.Required(), mcp.Description("Name of the Pipeline.")),
			),
			Handler: r.getPipeline,
		},
		{
			Tool: readOnlyTool("list_monovertices",
				mcp.WithDescription("List all Numaflow MonoVertices in a namespace. Each entry includes the MonoVertex's phase and its current/ready replica counts."),
				mcp.WithString("namespace", mcp.Description("Kubernetes namespace to list MonoVertices in. If omitted, the server's default namespace is used (empty default means all namespaces).")),
			),
			Handler: r.listMonoVertices,
		},
		{
			Tool: readOnlyTool("get_monovertex",
				mcp.WithDescription("Get the full spec and status of a single Numaflow MonoVertex."),
				mcp.WithString("namespace", mcp.Description("Namespace of the MonoVertex. If omitted, the server's default namespace is used.")),
				mcp.WithString("mono_vertex", mcp.Required(), mcp.Description("Name of the MonoVertex.")),
			),
			Handler: r.getMonoVertex,
		},
		{
			Tool: readOnlyTool("list_isbservices",
				mcp.WithDescription("List all Numaflow InterStepBufferServices (ISB services) in a namespace. Each entry includes the service's phase and type (e.g. jetstream)."),
				mcp.WithString("namespace", mcp.Description("Kubernetes namespace to list InterStepBufferServices in. If omitted, the server's default namespace is used (empty default means all namespaces).")),
			),
			Handler: r.listISBServices,
		},
	}
}

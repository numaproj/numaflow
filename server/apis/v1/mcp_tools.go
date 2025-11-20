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

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"k8s.io/client-go/kubernetes"
	metricsclientv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"

	// metricsclientv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1bta1"

	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

// ToolRegistry interface for registering tools
type ToolRegistry interface {
	RegisteredTools() []ToolDefinition
}

// ToolDefinition represents a tool with its handler
type ToolDefinition struct {
	Tool    mcp.Tool
	Handler server.ToolHandlerFunc
}

type toolkit struct {
	kubeClient             kubernetes.Interface
	numaflowClient         dfv1clients.NumaflowV1alpha1Interface
	metricsClient          metricsclientv1beta1.MetricsV1beta1Interface
	daemonClientsCache     *lru.Cache[string, daemonclient.DaemonClient]
	mvtxDaemonClientsCache *lru.Cache[string, mvtdaemonclient.MonoVertexDaemonClient]
}

func NewMCPToolkit(kubeClient kubernetes.Interface,
	numaflowClient dfv1clients.NumaflowV1alpha1Interface,
	metricsClient metricsclientv1beta1.MetricsV1beta1Interface,
	daemonClientsCache *lru.Cache[string, daemonclient.DaemonClient],
	mvtxDaemonClientsCache *lru.Cache[string, mvtdaemonclient.MonoVertexDaemonClient]) ToolRegistry {
	return &toolkit{
		kubeClient:             kubeClient,
		numaflowClient:         numaflowClient,
		metricsClient:          metricsClient,
		daemonClientsCache:     daemonClientsCache,
		mvtxDaemonClientsCache: mvtxDaemonClientsCache,
	}
}

var _ ToolRegistry = (*toolkit)(nil)

func (tk *toolkit) RegisteredTools() []ToolDefinition {
	return []ToolDefinition{
		{
			mcp.NewTool("list_pipelines",
				mcp.WithDescription("A tool to list all the Pipeline objects in a specified namespace, each Pipeline object also contains healthy status (healthy, unknown, critical, warning, inactive)"),
				mcp.WithString("namespace",
					mcp.Required(),
					mcp.Description("The namespace of the Pipelines"),
				),
			),
			tk.listPipelines,
		},
		{
			mcp.NewTool("list_monovertices",
				mcp.WithDescription("A tool to list all the MonoVertex objects in a specified namespace, each MonoVertex also contains healthy status (healthy, critical, warning)"),
				mcp.WithString("namespace",
					mcp.Required(),
					mcp.Description("The namespace of the MonoVertices"),
				),
			),
			tk.listMonoVertices,
		},
	}
}

func (tk *toolkit) listPipelines(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	namespace, err := request.RequireString("namespace")
	if err != nil {
		return mcp.NewToolResultError("namespace is required"), nil
	}
	pls, err := getPipelines(tk.numaflowClient, namespace)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	bs, err := json.Marshal(pls)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(bs)), nil
}

func (tk *toolkit) listMonoVertices(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	namespace, err := request.RequireString("namespace")
	if err != nil {
		return mcp.NewToolResultError("namespace is required"), nil
	}
	mvs, err := getMonoVertices(tk.numaflowClient, namespace)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	bs, err := json.Marshal(mvs)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(bs)), nil
}

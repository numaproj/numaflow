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
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// objectSummary is a compact, read-only view of a Numaflow object. It is kept
// intentionally small so that list results do not overwhelm the model's
// context window; use the get_* tools for the full object.
type objectSummary struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Phase     string `json:"phase,omitempty"`
	Message   string `json:"message,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
	// Type is populated for InterStepBufferServices (e.g. "jetstream").
	Type string `json:"type,omitempty"`
	// VertexCount is populated for Pipelines.
	VertexCount *uint32 `json:"vertexCount,omitempty"`
	// Replicas / ReadyReplicas are populated for MonoVertices.
	Replicas      *uint32 `json:"replicas,omitempty"`
	ReadyReplicas *uint32 `json:"readyReplicas,omitempty"`
}

func (r *registry) listPipelines(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns := r.namespace(req)
	list, err := r.numaflowClient.Pipelines(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list pipelines: %v", err)), nil
	}
	summaries := make([]objectSummary, 0, len(list.Items))
	for i := range list.Items {
		pl := &list.Items[i]
		vertexCount := uint32(len(pl.Spec.Vertices))
		summaries = append(summaries, objectSummary{
			Name:        pl.Name,
			Namespace:   pl.Namespace,
			Phase:       string(pl.Status.Phase),
			Message:     pl.Status.Message,
			CreatedAt:   created(pl.ObjectMeta),
			VertexCount: &vertexCount,
		})
	}
	return jsonResult(summaries)
}

func (r *registry) getPipeline(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("pipeline")
	if err != nil {
		return mcp.NewToolResultError("pipeline is required"), nil
	}
	ns := r.namespace(req)
	if ns == "" {
		return mcp.NewToolResultError("namespace is required (no default namespace configured)"), nil
	}
	pl, err := r.numaflowClient.Pipelines(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get pipeline %q in namespace %q: %v", name, ns, err)), nil
	}
	pl.ManagedFields = nil
	return jsonResult(pl)
}

func (r *registry) listMonoVertices(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns := r.namespace(req)
	list, err := r.numaflowClient.MonoVertices(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list monovertices: %v", err)), nil
	}
	summaries := make([]objectSummary, 0, len(list.Items))
	for i := range list.Items {
		mv := &list.Items[i]
		replicas := mv.Status.Replicas
		readyReplicas := mv.Status.ReadyReplicas
		summaries = append(summaries, objectSummary{
			Name:          mv.Name,
			Namespace:     mv.Namespace,
			Phase:         string(mv.Status.Phase),
			CreatedAt:     created(mv.ObjectMeta),
			Replicas:      &replicas,
			ReadyReplicas: &readyReplicas,
		})
	}
	return jsonResult(summaries)
}

func (r *registry) getMonoVertex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("mono_vertex")
	if err != nil {
		return mcp.NewToolResultError("mono_vertex is required"), nil
	}
	ns := r.namespace(req)
	if ns == "" {
		return mcp.NewToolResultError("namespace is required (no default namespace configured)"), nil
	}
	mv, err := r.numaflowClient.MonoVertices(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get monovertex %q in namespace %q: %v", name, ns, err)), nil
	}
	mv.ManagedFields = nil
	return jsonResult(mv)
}

func (r *registry) listISBServices(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns := r.namespace(req)
	list, err := r.numaflowClient.InterStepBufferServices(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list isb services: %v", err)), nil
	}
	summaries := make([]objectSummary, 0, len(list.Items))
	for i := range list.Items {
		isb := &list.Items[i]
		isbType := string(isb.Status.Type)
		if isbType == "" {
			isbType = string(isb.GetType())
		}
		summaries = append(summaries, objectSummary{
			Name:      isb.Name,
			Namespace: isb.Namespace,
			Phase:     string(isb.Status.Phase),
			Message:   isb.Status.Message,
			CreatedAt: created(isb.ObjectMeta),
			Type:      isbType,
		})
	}
	return jsonResult(summaries)
}

// created renders an object's creation timestamp as an RFC3339 UTC string.
func created(meta metav1.ObjectMeta) string {
	if meta.CreationTimestamp.IsZero() {
		return ""
	}
	return meta.CreationTimestamp.UTC().Format(time.RFC3339)
}

// jsonResult marshals v to indented JSON and wraps it as a tool text result.
func jsonResult(v any) (*mcp.CallToolResult, error) {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(bs)), nil
}

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
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaflow/server/apis/v1"
)

const (
	maxTailLines    = int64(10000)
	defaultEvtLimit = int64(200)

	isbSvcStatusHealthy  = "healthy"
	isbSvcStatusCritical = "critical"
	isbSvcStatusWarning  = "warning"
	isbSvcStatusInactive = "inactive"
)

// k8sRegistry adds a kubernetes.Interface to the base registry so Kubernetes-
// backed tools (namespaces, pods, logs, events, cluster summary) can be served.
type k8sRegistry struct {
	daemonRegistry
	kubeClient     kubernetes.Interface
	numaflowClient dfv1clients.NumaflowV1alpha1Interface
}

// k8sToolDefinitions returns read-only tool definitions for all Kubernetes-backed tools.
func (r *k8sRegistry) k8sToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{
			Tool: readOnlyTool("list_namespaces",
				mcp.WithDescription("List all Kubernetes namespaces visible to the server, excluding kube-system, kube-public, and kube-node-lease."),
			),
			Handler: r.listNamespaces,
		},
		{
			Tool: readOnlyTool("get_cluster_summary",
				mcp.WithDescription("Get a per-namespace summary of all Numaflow Pipelines, MonoVertices, and ISB Services in the cluster, with counts bucketed by health status (healthy/warning/critical/inactive)."),
			),
			Handler: r.getClusterSummary,
		},
		{
			Tool: readOnlyTool("list_pods",
				mcp.WithDescription("List pods backing a Numaflow Pipeline vertex or MonoVertex. Set kind to 'pipeline' or 'monovertex'."),
				mcp.WithString("namespace", mcp.Required(), mcp.Description("Kubernetes namespace.")),
				mcp.WithString("kind", mcp.Required(), mcp.Description("Resource kind: 'pipeline' or 'monovertex'.")),
				mcp.WithString("name", mcp.Required(), mcp.Description("Name of the Pipeline or MonoVertex.")),
				mcp.WithString("vertex", mcp.Description("Vertex name (only used when kind=pipeline).")),
			),
			Handler: r.listPods,
		},
		{
			Tool: readOnlyTool("get_pod_info",
				mcp.WithDescription("Get detailed status and container info for pods backing a Numaflow Pipeline vertex or MonoVertex. Includes restart counts, waiting/termination reasons, and resource requests/limits."),
				mcp.WithString("namespace", mcp.Required(), mcp.Description("Kubernetes namespace.")),
				mcp.WithString("kind", mcp.Required(), mcp.Description("Resource kind: 'pipeline' or 'monovertex'.")),
				mcp.WithString("name", mcp.Required(), mcp.Description("Name of the Pipeline or MonoVertex.")),
				mcp.WithString("vertex", mcp.Description("Vertex name (only used when kind=pipeline).")),
			),
			Handler: r.getPodInfo,
		},
		{
			Tool: readOnlyTool("tail_pod_logs",
				mcp.WithDescription("Fetch the last N lines of logs from a pod container. Maximum 10000 lines per call. Returns plain text with timestamps prepended by the Kubernetes log API."),
				mcp.WithString("namespace", mcp.Required(), mcp.Description("Kubernetes namespace.")),
				mcp.WithString("pod", mcp.Required(), mcp.Description("Pod name.")),
				mcp.WithString("container", mcp.Description("Container name (defaults to first container if omitted).")),
				mcp.WithNumber("lines", mcp.Description(fmt.Sprintf("Number of lines to return from the end of the log (max %d, default 100).", maxTailLines))),
				mcp.WithString("since", mcp.Description("Only return logs newer than this duration, e.g. '5m', '1h'.")),
				mcp.WithBoolean("previous", mcp.Description("Return logs from the previous (terminated) container instance.")),
			),
			Handler: r.tailPodLogs,
		},
		{
			Tool: readOnlyTool("list_namespace_events",
				mcp.WithDescription("List recent Kubernetes events for a namespace, sorted newest first. Optionally filter by object kind and name."),
				mcp.WithString("namespace", mcp.Required(), mcp.Description("Kubernetes namespace.")),
				mcp.WithString("object_type", mcp.Description("Filter by involved object kind, e.g. 'Pipeline', 'Pod'.")),
				mcp.WithString("object_name", mcp.Description("Filter by involved object name (requires object_type to also be set).")),
				mcp.WithNumber("limit", mcp.Description(fmt.Sprintf("Maximum events to return (default %d).", defaultEvtLimit))),
			),
			Handler: r.listNamespaceEvents,
		},
	}
}

func (r *k8sRegistry) listNamespaces(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	all, err := r.kubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list namespaces: %v", err)), nil
	}
	var names []string
	for _, ns := range all.Items {
		name := ns.Name
		if name == metav1.NamespaceSystem || name == metav1.NamespacePublic || name == "kube-node-lease" {
			continue
		}
		names = append(names, name)
	}
	return jsonResult(names)
}

func (r *k8sRegistry) getClusterSummary(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	type nsSummary struct {
		pl  apiv1.PipelineSummary
		isb apiv1.IsbServiceSummary
		mv  apiv1.MonoVertexSummary
	}
	summaryMap := make(map[string]*nsSummary)

	ensureNS := func(ns string) *nsSummary {
		if s, ok := summaryMap[ns]; ok {
			return s
		}
		s := &nsSummary{}
		summaryMap[ns] = s
		return s
	}

	// Pipelines
	plList, err := r.numaflowClient.Pipelines("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list pipelines: %v", err)), nil
	}
	for i := range plList.Items {
		pl := &plList.Items[i]
		status := pipelineHealthStatus(pl)
		s := ensureNS(pl.Namespace)
		if status == dfv1.PipelineStatusInactive || status == dfv1.PipelineStatusUnknown || status == dfv1.PipelineStatusDeleting {
			s.pl.Inactive++
		} else {
			incrementActive(&s.pl.Active, status)
		}
	}

	// ISB Services
	isbList, err := r.numaflowClient.InterStepBufferServices("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list isb services: %v", err)), nil
	}
	for i := range isbList.Items {
		isb := &isbList.Items[i]
		status := isbServiceHealthStatus(isb)
		s := ensureNS(isb.Namespace)
		if status == isbSvcStatusInactive {
			s.isb.Inactive++
		} else {
			incrementActive(&s.isb.Active, status)
		}
	}

	// MonoVertices
	mvList, err := r.numaflowClient.MonoVertices("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list mono vertices: %v", err)), nil
	}
	for i := range mvList.Items {
		mv := &mvList.Items[i]
		status := monoVertexHealthStatus(mv)
		s := ensureNS(mv.Namespace)
		if status == dfv1.MonoVertexStatusInactive || status == dfv1.MonoVertexStatusUnknown {
			s.mv.Inactive++
		} else {
			incrementActive(&s.mv.Active, status)
		}
	}

	// Collect and sort
	var resp apiv1.ClusterSummaryResponse
	for ns, s := range summaryMap {
		resp = append(resp, apiv1.NewNamespaceSummary(ns, s.pl, s.isb, s.mv))
	}
	sort.Slice(resp, func(i, j int) bool { return resp[i].Namespace < resp[j].Namespace })
	return jsonResult(resp)
}

func (r *k8sRegistry) listPods(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, _ := req.RequireString("namespace")
	pods, err := r.fetchPods(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	_ = ns
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	type podSummary struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Phase     string `json:"phase"`
		NodeName  string `json:"nodeName,omitempty"`
	}
	summaries := make([]podSummary, 0, len(pods))
	for _, p := range pods {
		summaries = append(summaries, podSummary{
			Name:      p.Name,
			Namespace: p.Namespace,
			Phase:     string(p.Status.Phase),
			NodeName:  p.Spec.NodeName,
		})
	}
	return jsonResult(summaries)
}

func (r *k8sRegistry) getPodInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pods, err := r.fetchPods(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	details := make([]apiv1.PodDetails, 0, len(pods))
	for _, p := range pods {
		details = append(details, podDetails(p))
	}
	return jsonResult(details)
}

func (r *k8sRegistry) tailPodLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, err := req.RequireString("namespace")
	if err != nil {
		return mcp.NewToolResultError("namespace is required"), nil
	}
	pod, err := req.RequireString("pod")
	if err != nil {
		return mcp.NewToolResultError("pod is required"), nil
	}

	lines := int64(100)
	if n := req.GetFloat("lines", 0); n > 0 {
		lines = int64(n)
	}
	if lines > maxTailLines {
		lines = maxTailLines
	}

	opts := &corev1.PodLogOptions{
		Container:  req.GetString("container", ""),
		TailLines:  &lines,
		Timestamps: true,
		Previous:   req.GetBool("previous", false),
	}

	if sinceStr := req.GetString("since", ""); sinceStr != "" {
		d, parseErr := time.ParseDuration(sinceStr)
		if parseErr == nil {
			sinceSeconds := int64(d.Seconds())
			opts.SinceSeconds = &sinceSeconds
		}
	}

	stream, err := r.kubeClient.CoreV1().Pods(ns).GetLogs(pod, opts).Stream(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get logs for pod %q: %v", pod, err)), nil
	}
	defer stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to read logs for pod %q: %v", pod, err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func (r *k8sRegistry) listNamespaceEvents(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, err := req.RequireString("namespace")
	if err != nil {
		return mcp.NewToolResultError("namespace is required"), nil
	}

	limit := defaultEvtLimit
	if n := req.GetFloat("limit", 0); n > 0 {
		limit = int64(n)
	}

	objType := req.GetString("object_type", "")
	objName := req.GetString("object_name", "")

	eventList, err := r.kubeClient.CoreV1().Events(ns).List(ctx, metav1.ListOptions{Limit: limit})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list events in namespace %q: %v", ns, err)), nil
	}

	var events []apiv1.K8sEventsResponse
	for _, evt := range eventList.Items {
		if evt.LastTimestamp.IsZero() {
			continue
		}
		if objType != "" && !strings.EqualFold(evt.InvolvedObject.Kind, objType) {
			continue
		}
		if objName != "" && !strings.EqualFold(evt.InvolvedObject.Name, objName) {
			continue
		}
		events = append(events, apiv1.NewK8sEventsResponse(
			evt.LastTimestamp.UnixMilli(),
			evt.Type,
			evt.InvolvedObject.Kind,
			evt.InvolvedObject.Name,
			evt.Reason,
			evt.Message,
		))
	}

	sort.Slice(events, func(i, j int) bool { return events[i].TimeStamp > events[j].TimeStamp })
	return jsonResult(events)
}

// fetchPods is shared by listPods and getPodInfo: derives the label selector
// from kind/name/vertex, does the List, and returns the pod slice.
func (r *k8sRegistry) fetchPods(ctx context.Context, req mcp.CallToolRequest) ([]corev1.Pod, error) {
	ns, err := req.RequireString("namespace")
	if err != nil {
		return nil, fmt.Errorf("namespace is required")
	}
	kind, err := req.RequireString("kind")
	if err != nil {
		return nil, fmt.Errorf("kind is required (pipeline or monovertex)")
	}
	name, err := req.RequireString("name")
	if err != nil {
		return nil, fmt.Errorf("name is required")
	}

	var selector string
	switch strings.ToLower(kind) {
	case "pipeline":
		vertex := req.GetString("vertex", "")
		if vertex != "" {
			selector = fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, name, dfv1.KeyVertexName, vertex)
		} else {
			selector = fmt.Sprintf("%s=%s", dfv1.KeyPipelineName, name)
		}
	case "monovertex":
		selector = fmt.Sprintf("%s=%s", dfv1.KeyMonoVertexName, name)
	default:
		return nil, fmt.Errorf("kind must be 'pipeline' or 'monovertex', got %q", kind)
	}

	pods, err := r.kubeClient.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}
	return pods.Items, nil
}

// podDetails builds a PodDetails from a pod without calling the metrics server
// (avoids an extra dependency; CPU/memory fields will be empty).
func podDetails(pod corev1.Pod) apiv1.PodDetails {
	d := apiv1.PodDetails{
		Name:    pod.Name,
		Status:  string(pod.Status.Phase),
		Message: pod.Status.Message,
		Reason:  pod.Status.Reason,
	}
	d.ContainerDetailsMap = containerDetails(pod)
	return d
}

func containerDetails(pod corev1.Pod) map[string]apiv1.ContainerDetails {
	m := make(map[string]apiv1.ContainerDetails)

	processStatus := func(status corev1.ContainerStatus, isInit bool) {
		if isInit && !isSidecar(status.Name, pod) {
			return
		}
		cd := apiv1.ContainerDetails{
			Name:         status.Name,
			ID:           status.ContainerID,
			State:        containerState(status.State),
			RestartCount: status.RestartCount,
		}
		if status.State.Waiting != nil {
			cd.WaitingReason = status.State.Waiting.Reason
			cd.WaitingMessage = status.State.Waiting.Message
		}
		if status.LastTerminationState.Terminated != nil {
			cd.LastTerminationReason = status.LastTerminationState.Terminated.Reason
			cd.LastTerminationMessage = status.LastTerminationState.Terminated.Message
			cd.LastTerminationExitCode = &status.LastTerminationState.Terminated.ExitCode
		}
		if status.State.Running != nil {
			cd.LastStartedAt = status.State.Running.StartedAt.Format(time.RFC3339)
		}
		m[status.Name] = cd
	}

	for _, s := range pod.Status.InitContainerStatuses {
		processStatus(s, true)
	}
	for _, s := range pod.Status.ContainerStatuses {
		processStatus(s, false)
	}

	processResources := func(c corev1.Container, isInit bool) {
		if isInit && !isSidecar(c.Name, pod) {
			return
		}
		cd, ok := m[c.Name]
		if !ok {
			cd = apiv1.ContainerDetails{Name: c.Name}
		}
		if v := c.Resources.Requests.Cpu().MilliValue(); v != 0 {
			cd.RequestedCPU = strconv.FormatInt(v, 10) + "m"
		}
		if v := c.Resources.Requests.Memory().Value() / (1024 * 1024); v != 0 {
			cd.RequestedMemory = strconv.FormatInt(v, 10) + "Mi"
		}
		if v := c.Resources.Limits.Cpu().MilliValue(); v != 0 {
			cd.LimitCPU = strconv.FormatInt(v, 10) + "m"
		}
		if v := c.Resources.Limits.Memory().Value() / (1024 * 1024); v != 0 {
			cd.LimitMemory = strconv.FormatInt(v, 10) + "Mi"
		}
		m[c.Name] = cd
	}

	for _, c := range pod.Spec.InitContainers {
		processResources(c, true)
	}
	for _, c := range pod.Spec.Containers {
		processResources(c, false)
	}
	return m
}

func isSidecar(name string, pod corev1.Pod) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.Name == name {
			return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
		}
	}
	return false
}

func containerState(state corev1.ContainerState) string {
	switch {
	case state.Running != nil:
		return "Running"
	case state.Waiting != nil:
		return "Waiting"
	case state.Terminated != nil:
		return "Terminated"
	default:
		return "Unknown"
	}
}

// pipelineHealthStatus mirrors getPipelineStatus in server/apis/v1/handler.go.
func pipelineHealthStatus(pl *dfv1.Pipeline) string {
	switch pl.Status.Phase {
	case dfv1.PipelinePhaseFailed:
		return dfv1.PipelineStatusCritical
	case dfv1.PipelinePhasePaused, dfv1.PipelinePhasePausing:
		return dfv1.PipelineStatusInactive
	case dfv1.PipelinePhaseDeleting:
		return dfv1.PipelineStatusDeleting
	case dfv1.PipelinePhaseRunning:
		if pl.GetDesiredPhase() == dfv1.PipelinePhasePaused {
			return dfv1.PipelineStatusInactive
		}
		if !pl.Status.IsHealthy() {
			return dfv1.PipelineStatusWarning
		}
		return dfv1.PipelineStatusHealthy
	default:
		return dfv1.PipelineStatusUnknown
	}
}

// isbServiceHealthStatus mirrors getIsbServiceStatus in server/apis/v1/handler.go.
func isbServiceHealthStatus(isb *dfv1.InterStepBufferService) string {
	switch isb.Status.Phase {
	case dfv1.ISBSvcPhaseFailed:
		return isbSvcStatusCritical
	case dfv1.ISBSvcPhaseUnknown:
		return isbSvcStatusInactive
	case dfv1.ISBSvcPhasePending, dfv1.ISBSvcPhaseRunning:
		if !isb.Status.IsHealthy() {
			return isbSvcStatusWarning
		}
		return isbSvcStatusHealthy
	default:
		return isbSvcStatusHealthy
	}
}

// monoVertexHealthStatus mirrors getMonoVertexStatus in server/apis/v1/handler.go.
func monoVertexHealthStatus(mv *dfv1.MonoVertex) string {
	switch mv.Status.Phase {
	case dfv1.MonoVertexPhaseFailed:
		return dfv1.MonoVertexStatusCritical
	case dfv1.MonoVertexPhasePaused:
		return dfv1.MonoVertexStatusInactive
	case dfv1.MonoVertexPhaseRunning:
		if mv.Spec.Lifecycle.GetDesiredPhase() == dfv1.MonoVertexPhasePaused {
			return dfv1.MonoVertexStatusInactive
		}
		if !mv.Status.IsHealthy() {
			return dfv1.MonoVertexStatusWarning
		}
		return dfv1.MonoVertexStatusHealthy
	default:
		return dfv1.MonoVertexStatusUnknown
	}
}

// incrementActive increments the correct bucket (healthy/warning/critical) on
// an ActiveStatus by the status string returned from the *HealthStatus helpers.
// All three resource types use the same string values ("healthy", "warning",
// "critical"), so a single switch on the string value suffices.
func incrementActive(a *apiv1.ActiveStatus, status string) {
	switch status {
	case "healthy":
		a.Healthy++
	case "warning":
		a.Warning++
	default:
		a.Critical++
	}
}

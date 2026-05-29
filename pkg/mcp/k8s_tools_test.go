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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dffake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	apiv1 "github.com/numaproj/numaflow/server/apis/v1"
)

func newK8sTestRegistry(t *testing.T) ToolRegistry {
	t.Helper()

	// Swap daemon factories so the registry can be constructed without a real cluster.
	origPl, origMv := daemonClientFactory, mvtDaemonClientFactory
	t.Cleanup(func() { daemonClientFactory = origPl; mvtDaemonClientFactory = origMv })
	daemonClientFactory = func(_, _ string) (daemonclient.DaemonClient, error) { return &fakeDaemonClient{}, nil }
	mvtDaemonClientFactory = func(_, _ string) (mvtdaemonclient.MonoVertexDaemonClient, error) {
		return &fakeMVTDaemonClient{}, nil
	}

	kube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: testNS,
				Labels: map[string]string{
					dfv1.KeyPipelineName: "p1",
					dfv1.KeyVertexName:   "in",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "evt-1", Namespace: testNS},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pipeline",
				Name: "p1",
			},
			Type:          "Normal",
			Reason:        "Created",
			Message:       "pipeline created",
			LastTimestamp: metav1.NewTime(time.Now()),
		},
	)

	pl := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: testNS},
		Spec:       dfv1.PipelineSpec{Vertices: []dfv1.AbstractVertex{{Name: "in"}, {Name: "out"}}},
		Status:     dfv1.PipelineStatus{Phase: dfv1.PipelinePhaseRunning},
	}
	isb := &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNS},
		Spec:       dfv1.InterStepBufferServiceSpec{JetStream: &dfv1.JetStreamBufferService{}},
		Status:     dfv1.InterStepBufferServiceStatus{Phase: dfv1.ISBSvcPhaseRunning},
	}
	client := dffake.NewSimpleClientset()
	nf := client.NumaflowV1alpha1()
	ctx := context.Background()
	_, err := nf.Pipelines(testNS).Create(ctx, pl, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = nf.InterStepBufferServices(testNS).Create(ctx, isb, metav1.CreateOptions{})
	require.NoError(t, err)

	return NewRegistry(kube, nf, testNS, "grpc")
}

func TestListNamespaces(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_namespaces"), map[string]any{})
	assert.False(t, res.IsError)
	var names []string
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &names))
	assert.Contains(t, names, testNS)
	assert.NotContains(t, names, "kube-system", "system namespaces must be filtered")
}

func TestGetClusterSummary(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_cluster_summary"), map[string]any{})
	assert.False(t, res.IsError)
	var summary apiv1.ClusterSummaryResponse
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &summary))
	require.NotEmpty(t, summary)
	var found bool
	for _, ns := range summary {
		if ns.Namespace == testNS {
			found = true
			total := ns.PipelineSummary.Active.Healthy + ns.PipelineSummary.Active.Warning + ns.PipelineSummary.Active.Critical
			assert.Equal(t, 1, total, "expected exactly one active pipeline in namespace %s", testNS)
		}
	}
	assert.True(t, found, "cluster summary must include %s", testNS)
}

func TestListPods(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_pods"), map[string]any{
		"namespace": testNS, "kind": "pipeline", "name": "p1", "vertex": "in",
	})
	assert.False(t, res.IsError)
	assert.Contains(t, textOf(t, res), "pod-1")
}

func TestListPods_InvalidKind(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_pods"), map[string]any{
		"namespace": testNS, "kind": "banana", "name": "p1",
	})
	assert.True(t, res.IsError)
}

func TestGetPodInfo(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "get_pod_info"), map[string]any{
		"namespace": testNS, "kind": "pipeline", "name": "p1", "vertex": "in",
	})
	assert.False(t, res.IsError)
	var details []apiv1.PodDetails
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &details))
	require.Len(t, details, 1)
	assert.Equal(t, "pod-1", details[0].Name)
}

func TestListNamespaceEvents(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_namespace_events"), map[string]any{"namespace": testNS})
	assert.False(t, res.IsError)
	var events []apiv1.K8sEventsResponse
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &events))
	require.NotEmpty(t, events)
	assert.Equal(t, "Pipeline/p1", events[0].Object)
}

func TestListNamespaceEvents_FilterByType(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "list_namespace_events"), map[string]any{
		"namespace":   testNS,
		"object_type": "Pipeline",
		"object_name": "p1",
	})
	assert.False(t, res.IsError)
	var events []apiv1.K8sEventsResponse
	require.NoError(t, json.Unmarshal([]byte(textOf(t, res)), &events))
	for _, e := range events {
		assert.Equal(t, "Pipeline/p1", e.Object)
	}
}

func TestTailPodLogs_MissingPod(t *testing.T) {
	reg := newK8sTestRegistry(t)
	res := call(t, handlerFor(t, reg, "tail_pod_logs"), map[string]any{"namespace": testNS})
	assert.True(t, res.IsError)
}

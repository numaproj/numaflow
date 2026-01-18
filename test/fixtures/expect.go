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

package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

type Expect struct {
	t                *testing.T
	isbSvcClient     flowpkg.InterStepBufferServiceInterface
	pipelineClient   flowpkg.PipelineInterface
	vertexClient     flowpkg.VertexInterface
	monoVertexClient flowpkg.MonoVertexInterface
	isbSvc           *dfv1.InterStepBufferService
	pipeline         *dfv1.Pipeline
	servingPipeline  *dfv1.ServingPipeline
	monoVertex       *dfv1.MonoVertex
	restConfig       *rest.Config
	kubeClient       kubernetes.Interface
}

// RedisSinkContains checks if the target string is written to the redis sink
// hashKey is the hash key environment variable set by the sink
// targetStr is the target string to check
func (t *Expect) RedisSinkContains(hashKey string, targetStr string, opts ...SinkCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	if contains := redisContains(ctx, hashKey, targetStr, opts...); !contains {
		t.t.Fatalf("Expected redis contains target string %s written under hash key %s.", targetStr, hashKey)
	}
	return t
}

// RedisSinkNotContains checks if the target string is not written to the redis sink
// hashKey is the hash key environment variable set by the sink
// targetStr is the target string to check
func (t *Expect) RedisSinkNotContains(hashKey string, targetStr string, opts ...SinkCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	if notContain := redisNotContains(ctx, hashKey, targetStr, opts...); !notContain {
		t.t.Fatalf("Not expected redis contains target string %s written under hash key %s.", targetStr, hashKey)
	}
	return t
}

func (t *Expect) ISBSvcDeleted(timeout time.Duration) *Expect {
	t.t.Helper()
	ctx := context.Background()
	_, err := t.isbSvcClient.Get(ctx, t.isbSvc.Name, metav1.GetOptions{})
	if err == nil || !apierr.IsNotFound(err) {
		t.t.Fatalf("Expected ISB svc to be deleted: %v", err)
	}

	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyManagedBy, dfv1.ControllerISBSvc, dfv1.KeyISBSvcName, ISBSvcName)
	opts := metav1.ListOptions{LabelSelector: labelSelector}
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()
	for {
		podList, err := t.kubeClient.CoreV1().Pods(Namespace).List(ctx, opts)
		if err != nil && !apierr.IsNotFound(err) {
			t.t.Fatalf("Failed to check if ISB svc pods have been deleted: %v", err)
		}
		if len(podList.Items) == 0 {
			return t
		}
		select {
		case <-timeoutCh:
			t.t.Fatalf("Timeout after %v waiting for ISB svc to be deleted", timeout)
		default:
		}
	}
}

func (t *Expect) VertexPodsRunning() *Expect {
	t.t.Helper()
	ctx := context.Background()
	for _, v := range t.pipeline.Spec.Vertices {
		_, err := t.vertexClient.Get(ctx, t.pipeline.Name+"-"+v.Name, metav1.GetOptions{})
		if err != nil {
			t.t.Fatalf("Expected vertex %q existing: %v", v.Name, err)
		}
	}
	// check pods running
	timeout := 3 * time.Minute
	for _, v := range t.pipeline.Spec.Vertices {
		if err := WaitForVertexPodRunning(t.kubeClient, t.vertexClient, Namespace, t.pipeline.Name, v.Name, timeout); err != nil {
			t.t.Fatalf("Expected vertex %q pod running: %v", v.Name, err)
		}
	}
	return t
}

func (t *Expect) ServingPodsRunning() *Expect {
	t.t.Helper()
	// Wait for serving server deployment pods
	timeout := 2 * time.Minute
	if err := WaitForServingServerPodsRunning(t.kubeClient, Namespace, t.servingPipeline.Name, timeout); err != nil {
		t.t.Fatalf("Expected serving server pods of %q running: %v", t.servingPipeline.Name, err)
	}
	// Wait for internal pipeline vertex pods (pipeline name is s-<serving-name>)
	plName := t.servingPipeline.GetPipelineName()
	for _, v := range t.servingPipeline.Spec.Pipeline.Vertices {
		if err := WaitForVertexPodRunning(t.kubeClient, t.vertexClient, Namespace, plName, v.Name, timeout); err != nil {
			t.t.Fatalf("Expected vertex %q pod running in serving pipeline: %v", v.Name, err)
		}
	}
	return t
}

func (t *Expect) MonoVertexPodsRunning() *Expect {
	t.t.Helper()
	timeout := 3 * time.Minute
	if err := WaitForMonoVertexPodRunning(t.kubeClient, t.monoVertexClient, Namespace, t.monoVertex.Name, timeout); err != nil {
		t.t.Fatalf("Expected mono vertex %q pod running: %v", t.monoVertex.Name, err)
	}
	return t
}

func (t *Expect) VertexSizeScaledTo(v string, size int) *Expect {
	t.t.Helper()
	ctx := context.Background()
	if _, err := t.vertexClient.Get(ctx, t.pipeline.Name+"-"+v, metav1.GetOptions{}); err != nil {
		t.t.Fatalf("Expected vertex %s existing: %v", v, err)
	}

	// check expected number of pods running
	timeout := 2 * time.Minute
	if err := WaitForVertexPodScalingTo(t.kubeClient, t.vertexClient, Namespace, t.pipeline.Name, v, timeout, size); err != nil {
		t.t.Fatalf("Expected %d pods running on vertex %s : %v", size, v, err)
	}
	return t
}

func (t *Expect) VertexPodLogContains(vertexName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := VertexPodLogContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, vertexName, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check vertex %q pod logs: %v", vertexName, err)
	}
	if !contains {
		t.t.Fatalf("Expected vertex [%q] pod log to contain [%q] but didn't.", vertexName, regex)
	}
	t.t.Logf("Expected vertex %q pod contains %q", vertexName, regex)
	return t
}

func (t *Expect) MonoVertexPodLogContains(regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := MonoVertexPodLogContains(ctx, t.kubeClient, Namespace, t.monoVertex.Name, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check mono vertex %q pod logs: %v", t.monoVertex.Name, err)
	}
	if !contains {
		t.t.Fatalf("Expected mono vertex [%q] pod log to contain [%q] but didn't.", t.monoVertex.Name, regex)
	}
	t.t.Logf("Expected mono vertex %q pod contains %q", t.monoVertex.Name, regex)
	return t
}

func (t *Expect) MonoVertexPodLogNotContains(regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	yes, err := MonoVertexPodLogNotContains(ctx, t.kubeClient, Namespace, t.monoVertex.Name, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check mono-vertex pod logs: %v", err)
	}
	if !yes {
		t.t.Fatalf("Not expected mono-vertex pod log contains %q", regex)
	}
	t.t.Logf("Expected mono vertex %q pod to not contain %q", t.monoVertex.Name, regex)
	return t
}

func (t *Expect) VertexPodLogNotContains(vertexName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	yes, err := VertexPodLogNotContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, vertexName, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check vertex pod logs: %v", err)
	}
	if !yes {
		t.t.Fatalf("Not expected vertex %q pod log contains %q", vertexName, regex)
	}
	return t
}

func (t *Expect) DaemonPodsRunning() *Expect {
	t.t.Helper()
	timeout := 2 * time.Minute
	if err := WaitForDaemonPodsRunning(t.kubeClient, Namespace, t.pipeline.Name, timeout); err != nil {
		t.t.Fatalf("Expected daemon pods of pipeline %q running: %v", t.pipeline.Name, err)
	}
	return t
}

func (t *Expect) DaemonPodLogContains(pipelineName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := DaemonPodLogContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check daemon pod logs: %v", err)
	}
	if !contains {
		t.t.Fatalf("Expected daemon pod log contains %q", regex)
	}
	return t
}

func (t *Expect) MvtxDaemonPodsRunning() *Expect {
	t.t.Helper()
	timeout := 2 * time.Minute
	if err := WaitForMvtxDaemonPodsRunning(t.kubeClient, Namespace, t.monoVertex.Name, timeout); err != nil {
		t.t.Fatalf("Expected mvtx daemon pods of %q running: %v", t.monoVertex.Name, err)
	}
	return t
}

func (t *Expect) When() *When {
	return &When{
		t:                t.t,
		isbSvcClient:     t.isbSvcClient,
		pipelineClient:   t.pipelineClient,
		vertexClient:     t.vertexClient,
		monoVertexClient: t.monoVertexClient,
		isbSvc:           t.isbSvc,
		pipeline:         t.pipeline,
		monoVertex:       t.monoVertex,
		restConfig:       t.restConfig,
		kubeClient:       t.kubeClient,
	}
}
